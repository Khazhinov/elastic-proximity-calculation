package elastic

import (
	"elastic-proximity-calculation/src/logger"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"os"
	"strings"
	"time"
)

type Config struct {
	Scheme       string
	Address      string
	Port         string
	Username     string
	Password     string
	LoggerEnable bool
}

func NewElasticConfig() Config {
	return Config{
		LoggerEnable: false,
	}
}

func GetElasticsearchClient(config Config) *elasticsearch.Client {
	retryBackoff := backoff.NewExponentialBackOff()

	var address strings.Builder
	address.WriteString(config.Scheme)
	address.WriteString(`://`)
	address.WriteString(config.Address)
	address.WriteString(`:`)
	address.WriteString(config.Port)

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

	if config.Username != "" && config.Password != "" {
		cfg.Username = config.Username
		cfg.Password = config.Password
	}

	if config.LoggerEnable {
		cfg.Logger = &estransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logger.Error("Не удалось создать клиент Elasticsearch: %s", err.Error())
	}

	return es
}
