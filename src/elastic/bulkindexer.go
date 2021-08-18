package elastic

import (
	"context"
	"elastic-proximity-calculation/src/logger"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"strconv"
	"time"
)

var bulkIndexers map[string]esutil.BulkIndexer = map[string]esutil.BulkIndexer{}

// GetBulkIndexer Функция возвращает esutil.BulkIndexer настроенный на массового индексирования в конкретный языковой индекс
func GetBulkIndexer(client *elasticsearch.Client, proximityIndexPrefix string, language string, proximityAmbit int) esutil.BulkIndexer {
	key := proximityIndexPrefix + language + "_proximity_" + strconv.Itoa(proximityAmbit)

	if _, ok := bulkIndexers[key]; !ok {
		tmpBulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:         key,
			Client:        client,
			FlushInterval: 30 * time.Second,
		})

		if err != nil {
			logger.Error("Не удалось создать BulkIndexer: %s", err.Error())
		}

		bulkIndexers[key] = tmpBulkIndexer
	}

	return bulkIndexers[key]
}

// CloseBulkIndexers Функция для закрытия всех существующих BulkIndexer
func CloseBulkIndexers() {
	for _, indexer := range bulkIndexers {
		if err := indexer.Close(context.Background()); err != nil {
			logger.Error("Не удалось закрыть BulkIndexer: %s", err.Error())
		}
	}
}
