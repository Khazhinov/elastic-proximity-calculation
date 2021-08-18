package calculator

import (
	"bytes"
	"context"
	"elastic-proximity-calculation/src/elastic"
	"elastic-proximity-calculation/src/helpers"
	"elastic-proximity-calculation/src/logger"
	"elastic-proximity-calculation/src/structs"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/tidwall/gjson"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	uploadsCount          int            = 1
	uploadsDocsCount      int            = 0
	uploadsDocsTotalCount int64          = 0
	totalSuccessUploads   int64          = 0
	totalErrorUploads     int64          = 0
	totalDocsCount        int64          = 0
	wg                    sync.WaitGroup = sync.WaitGroup{}
	re                    *regexp.Regexp = regexp.MustCompile(`([0-9]*[.,]*[0-9]+)|\p{L}+`)
	proximities                          = structs.NewContainer()
	client                *elasticsearch.Client

	config Config
)

func Do(initConfig Config) {
	config = initConfig

	client = elastic.GetElasticsearchClient(config.Elastic)

	keepAliveNew := time.Duration(config.KeepAlive) * time.Minute

	res, _ := client.Search(
		client.Search.WithIndex(config.SourceIndex),
		client.Search.WithSort("common.publication_date"),
		client.Search.WithSize(config.PageSize),
		client.Search.WithScroll(keepAliveNew),
	)

	j := helpers.ReaderToString(res.Body)
	res.Body.Close()

	scrollID := gjson.Get(j, "_scroll_id").String()

	totalDocsCount = gjson.Get(j, "hits.total.value").Int()

	hits := gjson.Get(j, "hits.hits")
	wg.Add(1)
	go processHits(hits)

	for {
		res, err := client.Scroll(client.Scroll.WithScrollID(scrollID), client.Scroll.WithScroll(keepAliveNew))

		if err != nil {
			logger.Error(err.Error())
		}
		if res.IsError() {
			logger.Error("Ошибка в ответе: %s", res.String())
		}

		j := helpers.ReaderToString(res.Body)
		res.Body.Close()

		scrollID = gjson.Get(j, "_scroll_id").String()
		hits := gjson.Get(j, "hits.hits")

		if len(hits.Array()) < 1 {
			wg.Wait()
			upload(config.Start)
			break
		} else {
			wg.Add(1)
			go processHits(hits)
		}

		if proximities.CheckTotalLength(config.UploadChunkSize) {
			wg.Wait()
			upload(config.Start)
		}
	}

	elastic.CloseBulkIndexers()

	dur := time.Since(config.Start)
	logger.Info("Выполнено. Общее время выполнения: %s", dur.String())
	logger.Info("Общее количество успешных загрузок: %s", strconv.FormatInt(totalSuccessUploads, 10))
	logger.Info("Общее количество неудачных загрузок: %s", strconv.FormatInt(totalErrorUploads, 10))
}

func upload(startTime time.Time) {
	logger.Info("Начало загрузки [%s]", strconv.Itoa(uploadsCount))

	for language, currentProximities := range proximities.GetAll() {
		bi := elastic.GetBulkIndexer(client, config.ProximityIndexPrefix, language, config.ProximityAmbit)

		var countSuccessful uint64
		start := time.Now().UTC()

		for _, proximity := range currentProximities {
			data, err := json.Marshal(proximity)
			if err != nil {
				logger.Error("Ошибка кодирования JSON")
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
						tmp := helpers.ReaderToString(item.Body)
						sourceId := gjson.Get(tmp, "source_id").String()

						if err != nil {
							logger.Warning(fmt.Sprintf("Ошибка при загрузке [ID исходного документа: %s]: %s", sourceId, err.Error()))
						} else {
							logger.Warning(fmt.Sprintf("Ошибка при загрузке [ID исходного документа: %s]: %s: %s", sourceId, res.Error.Type, res.Error.Reason))
						}
					},
				},
			)

			if err != nil {
				logger.Error("Необработанная ошибка: %s", err.Error())
			}

			proximity = nil
		}

		biStats := bi.Stats()
		dur := time.Since(start)

		if biStats.NumFailed > 0 {
			logger.Warning(
				fmt.Sprintf(
					"Для языка [%s] индексировано [%s] окрестностей с [%s] ошибками за %s (%s документов в секунду)",
					language,
					humanize.Comma(int64(biStats.NumFlushed)),
					humanize.Comma(int64(biStats.NumFailed)),
					dur.Truncate(time.Millisecond).String(),
					humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
				),
			)
		} else {
			logger.Info(
				fmt.Sprintf(
					"Для языка [%s] индексировано [%s] окрестностей за %s (%s документов в секунду)",
					language,
					humanize.Comma(int64(biStats.NumFlushed)),
					dur.Truncate(time.Millisecond).String(),
					humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
				),
			)
		}

		proximities.DeleteByLanguage(language)
	}

	logger.Info("Обработано документов за цикл: %s", strconv.Itoa(uploadsDocsCount))
	uploadsDocsCount = 0

	logger.Info(
		fmt.Sprintf(
			"Общее колличество обработанных документов: %s%% [%s/%s]",
			fmt.Sprintf("%.1f", math.Floor((float64(uploadsDocsTotalCount)/float64(totalDocsCount))*100)),
			strconv.FormatInt(uploadsDocsTotalCount, 10),
			strconv.FormatInt(totalDocsCount, 10),
		),
	)

	dur := time.Since(startTime)
	logger.Info("Скрипт выполняется: %s", dur.Truncate(time.Second).String())

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
	tokens := re.FindAll([]byte(textField), -1)
	tokensLength := len(tokens)

	for i := 0; i < tokensLength; i++ {
		currentToken := tokens[i]
		if helpers.IsNumber(string(currentToken)) {
			currentNumber := helpers.StringToFloat64(string(currentToken))
			currentProximity := structs.CreateProximityObject(config.SourceIndex, sourceDocId, sourceField, currentNumber)

			var (
				left  int
				right int
			)

			if i-config.ProximityAmbit > 0 {
				left = i - config.ProximityAmbit
			} else {
				left = 0
			}

			if i+config.ProximityAmbit < tokensLength {
				right = i + config.ProximityAmbit
			} else {
				right = tokensLength - 1
			}

			for j := left; j <= right; j++ {
				if j < i {
					var needleIndex string = strconv.Itoa(getNeedleIndex(i, j))
					if helpers.IsNumber(string(tokens[j])) {
						tmpNumber := helpers.StringToFloat64(string(tokens[j]))
						currentProximity["nb_"+needleIndex] = tmpNumber
						currentProximity["tb_"+needleIndex] = tokens[j]
					} else {
						currentProximity["tb_"+needleIndex] = tokens[j]
					}
				} else if j > i {
					var needleIndex string = strconv.Itoa(getNeedleIndex(i, j))
					if helpers.IsNumber(string(tokens[j])) {
						tmpNumber := helpers.StringToFloat64(string(tokens[j]))
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

func getNeedleIndex(center int, current int) int {
	if current < center {
		return center - current
	} else if current > center {
		return current - center
	} else {
		return 0
	}
}
