package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/joho/godotenv"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/scanner"
	"time"
)

var (
	elasticScheme       = env("ELASTIC_SCHEME", "http")
	elasticAddress      = env("ELASTIC_ADDRESS", "127.0.0.1")
	elasticPort         = env("ELASTIC_PORT", "9200")
	elasticUsername     = env("ELASTIC_USERNAME")
	elasticPassword     = env("ELASTIC_PASSWORD")
	elasticLoggerEnable = false
)

var (
	proximityAmbit, _    = strconv.Atoi(env("PROXIMITY_AMBIT", "15"))
	keepAlive, _         = strconv.Atoi(env("SCROLL_KEEP_ALIVE", "5"))
	sourceIndex          = env("SOURCE_INDEX")
	proximityIndexPrefix = env("TARGET_INDEX_PREFIX")
	pageSize, _          = strconv.Atoi(env("SINGLE_PAGE_SIZE", "1000"))
	uploadChunkSize, _   = strconv.Atoi(env("UPLOAD_CHUNK_SIZE", "1000000"))
)

var client *elasticsearch.Client = getElasticsearchClient()

var uploadsCount int = 1
var uploadsDocsCount int = 0
var uploadsDocsTotalCount int64 = 0
var totalSuccessUploads int64 = 0
var totalErrorUploads int64 = 0
var totalDocsCount int64 = 0
var wg sync.WaitGroup = sync.WaitGroup{}
var re *regexp.Regexp = regexp.MustCompile(`([0-9]*[.,]*[0-9]+)|\p{L}+`)
var proximities = NewContainer()
var bulkIndexers map[string]esutil.BulkIndexer = map[string]esutil.BulkIndexer{}

type Proximity map[string]interface{}

type Container struct {
	mx sync.RWMutex
	m  map[string][]*Proximity
}

func (c *Container) Add(language string, proximity *Proximity) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.m[language] = append(c.m[language], proximity)
}

func (c *Container) CheckTotalLength() bool {
	c.mx.RLock()
	defer c.mx.RUnlock()

	var total int = 0
	for _, proximities := range c.m {
		total += len(proximities)
	}

	return total >= uploadChunkSize
}

func (c *Container) GetByLanguage(language string) []*Proximity {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.m[language]
}

func (c *Container) GetAll() map[string][]*Proximity {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.m
}

func (c *Container) DeleteByLanguage(language string) {
	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.m, language)
}

func NewContainer() *Container {
	return &Container{
		m: make(map[string][]*Proximity),
	}
}

func main() {
	log.SetFlags(0)
	startTime := time.Now()
	fmt.Println("Start in ", startTime.String())

	keepAliveNew := time.Duration(keepAlive) * time.Minute

	res, _ := client.Search(
		client.Search.WithIndex(sourceIndex),
		client.Search.WithSort("common.publication_date"),
		client.Search.WithSize(pageSize),
		client.Search.WithScroll(keepAliveNew),
	)

	j := read(res.Body)
	res.Body.Close()

	scrollID := gjson.Get(j, "_scroll_id").String()

	totalDocsCount = gjson.Get(j, "hits.total.value").Int()

	hits := gjson.Get(j, "hits.hits")
	wg.Add(1)
	go processHits(hits)

	for {
		res, err := client.Scroll(client.Scroll.WithScrollID(scrollID), client.Scroll.WithScroll(keepAliveNew))

		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		if res.IsError() {
			log.Fatalf("Error response: %s", res)
		}

		j := read(res.Body)
		res.Body.Close()

		scrollID = gjson.Get(j, "_scroll_id").String()
		hits := gjson.Get(j, "hits.hits")

		if len(hits.Array()) < 1 {
			wg.Wait()
			upload(startTime)
			break
		} else {
			wg.Add(1)
			go processHits(hits)
		}

		if proximities.CheckTotalLength() {
			wg.Wait()
			upload(startTime)
		}
	}

	closeBulkIndexers()

	dur := time.Since(startTime)
	fmt.Println("Done. Duration: ", dur.String())
	fmt.Println("Total successful uploads: ", totalSuccessUploads)
	fmt.Println("Total error uploads: ", totalErrorUploads)
}

func closeBulkIndexers() {
	for language, _ := range proximities.GetAll() {
		bi := getBulkIndexerByLanguage(language)

		if err := bi.Close(context.Background()); err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}
}

func upload(startTime time.Time) {
	fmt.Printf(strings.Repeat("~", 15) + "\n")
	currentTime := time.Now()
	fmt.Printf("Upload [%d] start in time %s\n", uploadsCount, currentTime.String())

	for language, currentProximities := range proximities.GetAll() {
		bi := getBulkIndexerByLanguage(language)

		var countSuccessful uint64
		start := time.Now().UTC()

		for _, proximity := range currentProximities {
			data, err := json.Marshal(proximity)
			if err != nil {
				dd(proximity)
				log.Fatalf("Encode error")
			}

			err = bi.Add(
				context.Background(),
				esutil.BulkIndexerItem{
					Action: "index",
					Body:   bytes.NewReader(data),

					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						atomic.AddUint64(&countSuccessful, 1)
						totalSuccessUploads++
					},

					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						totalErrorUploads++
						tmp := read(item.Body)
						sourceId := gjson.Get(tmp, "source_id").String()

						if err != nil {
							log.Printf("ERROR [doc: %s]: %s", sourceId, err)
						} else {
							log.Printf("ERROR [doc: %s]: %s: %s", sourceId, res.Error.Type, res.Error.Reason)
						}
					},
				},
			)

			if err != nil {
				log.Fatalf("Unexpected error: %s", err)
			}

			proximity = nil
		}

		biStats := bi.Stats()
		dur := time.Since(start)

		if biStats.NumFailed > 0 {
			fmt.Printf(
				"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)\n",
				humanize.Comma(int64(biStats.NumFlushed)),
				humanize.Comma(int64(biStats.NumFailed)),
				dur.Truncate(time.Millisecond),
				humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
		} else {
			fmt.Printf(
				"Sucessfuly indexed [%s] documents in %s (%s docs/sec)\n",
				humanize.Comma(int64(biStats.NumFlushed)),
				dur.Truncate(time.Millisecond),
				humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
		}

		proximities.DeleteByLanguage(language)
	}
	fmt.Println("Documents processed by loop: ", uploadsDocsCount)
	uploadsDocsCount = 0

	fmt.Println("Documents processed total: ", math.Floor((float64(uploadsDocsTotalCount)/float64(totalDocsCount))*100), "% [", uploadsDocsTotalCount, "/", totalDocsCount, "]")

	dur := time.Since(startTime)
	fmt.Printf("Time has passed since the beginning %s\n", dur.Truncate(time.Second))

	uploadsCount++
	runtime.GC()
}

func processHits(hits gjson.Result) bool {
	defer wg.Done()

	hitsArray := hits.Array()
	if len(hitsArray) < 1 {
		return false
	} else {
		for i, result := range hitsArray {
			processSingleHit(i, result)
		}
		return true
	}
}

func processSingleHit(i int, hit gjson.Result) {
	needleFields := [3]string{"description_cleaned", "claims_cleaned", "abstract_cleaned"}

	if hit.Exists() {
		sourceDocId := gjson.Get(hit.Raw, "_id").String()
		for _, needleField := range needleFields {
			if tmpField := gjson.Get(hit.Raw, "_source."+needleField); tmpField.Exists() {
				for language, textField := range tmpField.Map() {
					calculateProximity(sourceDocId, needleField, language, textField.String())
				}
			}
		}
		uploadsDocsTotalCount++
		uploadsDocsCount++
	}

}

func calculateProximity(sourceDocId string, sourceField string, language string, textField string) {
	var s scanner.Scanner
	s.Init(strings.NewReader(textField))

	tokens := []string{}

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		token := s.TokenText()
		compileToken := string(re.Find([]byte(token)))
		if "" != compileToken {
			tokens = append(tokens, compileToken)
		}
	}

	tokensLength := len(tokens)

	for i := 0; i < tokensLength; i++ {
		currentToken := tokens[i]
		if isNumber(currentToken) {
			currentNumber := stringToFloat(currentToken)
			currentProximity := createProximityObject(sourceDocId, sourceField, currentNumber)

			var (
				left  int
				right int
			)

			if i-proximityAmbit > 0 {
				left = i - proximityAmbit
			} else {
				left = 0
			}

			if i+proximityAmbit < tokensLength {
				right = i + proximityAmbit
			} else {
				right = tokensLength - 1
			}

			for j := left; j <= right; j++ {
				if j < i {
					var needleIndex string = strconv.Itoa(getNeedleIndex(i, j))
					if isNumber(tokens[j]) {
						tmpNumber := stringToFloat(tokens[j])
						currentProximity["nb_"+needleIndex] = tmpNumber
						currentProximity["tb_"+needleIndex] = tokens[j]
					} else {
						currentProximity["tb_"+needleIndex] = tokens[j]
					}
				} else if j > i {
					var needleIndex string = strconv.Itoa(getNeedleIndex(i, j))
					if isNumber(tokens[j]) {
						tmpNumber := stringToFloat(tokens[j])
						currentProximity["na_"+needleIndex] = tmpNumber
						currentProximity["ta_"+needleIndex] = tokens[j]
					} else {
						currentProximity["ta_"+needleIndex] = tokens[j]
					}
				}
			}

			proximities.Add(language, &currentProximity)
		}
	}
}

func getBulkIndexerByLanguage(language string) esutil.BulkIndexer {
	key := proximityIndexPrefix + language + "_proximity_" + strconv.Itoa(proximityAmbit)

	if _, ok := bulkIndexers[key]; !ok {
		tmpBulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:         key,
			Client:        client,
			FlushInterval: 30 * time.Second,
		})

		if err != nil {
			log.Fatalf("Error creating the BulkIndexer: %s", err)
		}

		bulkIndexers[key] = tmpBulkIndexer
	}

	return bulkIndexers[key]
}

func dd(args ...interface{}) {
	fmt.Printf("%#v\n", args)
	os.Exit(0)
}

func createProximityObject(sourceId string, sourceField string, num float64) Proximity {
	return Proximity{
		"source_index": sourceIndex,
		"source_id":    sourceId,
		"source_field": sourceField,
		"num":          num,
	}
}

func getNeedleIndex(center int, current int) int {
	if current < center {
		return center - current
	} else if current > center {
		return current - center
	} else {
		return 0
	}
}

func stringToFloat(stringNumber string) float64 {
	num, _ := strconv.ParseFloat(stringNumber, 64)
	num = Round(num, 5)

	return num
}

func Round(x float64, prec int) float64 {
	var rounder float64
	pow := math.Pow(10, float64(prec))
	interred := x * pow
	_, frac := math.Modf(interred)
	if frac >= 0.5 {
		rounder = math.Ceil(interred)
	} else {
		rounder = math.Floor(interred)
	}

	return rounder / pow
}

func isNumber(token string) bool {
	if num, err := strconv.ParseFloat(token, 64); err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) && !math.IsInf(num, -1) && !math.IsInf(num, 1) && num != -0 {
		return true
	}

	return false
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

func getElasticsearchClient() *elasticsearch.Client {
	retryBackoff := backoff.NewExponentialBackOff()

	var address strings.Builder
	address.WriteString(elasticScheme)
	address.WriteString(`://`)
	address.WriteString(elasticAddress)
	address.WriteString(`:`)
	address.WriteString(elasticPort)

	cfg := elasticsearch.Config{
		Addresses: []string{
			address.String(),
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 3,
	}

	if elasticUsername != "" && elasticPassword != "" {
		cfg.Username = elasticUsername
		cfg.Password = elasticPassword
	}

	if elasticLoggerEnable {
		cfg.Logger = &estransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	return es
}

func env(name string, defaultVal ...string) string {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	tmp := os.Getenv(name)

	if tmp == "" {
		if len(defaultVal) > 0 {
			return defaultVal[0]
		} else {
			return ""
		}
	} else {
		return tmp
	}
}
