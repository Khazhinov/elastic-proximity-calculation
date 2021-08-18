package main

import (
	"elastic-proximity-calculation/src/calculator"
	"elastic-proximity-calculation/src/elastic"
	"elastic-proximity-calculation/src/helpers"
	"elastic-proximity-calculation/src/logger"
	"flag"
	"fmt"
	"strconv"
	"time"
)

var (
	Scheme       string
	Address      string
	Port         string
	Username     string
	Password     string
	LoggerEnable bool

	proximityAmbit       int
	keepAlive            int
	sourceIndex          string
	proximityIndexPrefix string
	pageSize             int
	uploadChunkSize      int

	config calculator.Config
)

func initWithFlags() {
	SchemeEnv := helpers.Env("ELASTIC_SCHEME", "http")
	flag.StringVar(&Scheme, "ELASTIC_SCHEME", SchemeEnv, "HTTP-схема для подключения к Elasticsearch.")

	AddressEnv := helpers.Env("ELASTIC_ADDRESS", "127.0.0.1")
	flag.StringVar(&Address, "ELASTIC_ADDRESS", AddressEnv, "Адрес для подключения к Elasticsearch.")

	PortEnv := helpers.Env("ELASTIC_PORT", "9200")
	flag.StringVar(&Port, "ELASTIC_PORT", PortEnv, "Порт для подключения к Elasticsearch.")

	UsernameEnv := helpers.Env("ELASTIC_USERNAME")
	flag.StringVar(&Username, "ELASTIC_USERNAME", UsernameEnv, "Пользователь для подключения к Elasticsearch.")

	PasswordEnv := helpers.Env("ELASTIC_PASSWORD")
	flag.StringVar(&Password, "ELASTIC_PASSWORD", PasswordEnv, "Пароль для подключения к Elasticsearch.")

	sourceIndexEnv := helpers.Env("SOURCE_INDEX")
	flag.StringVar(&sourceIndex, "SOURCE_INDEX", sourceIndexEnv, "Индекс источник.")

	proximityIndexPrefixEnv := helpers.Env("TARGET_INDEX_PREFIX")
	flag.StringVar(&proximityIndexPrefix, "TARGET_INDEX_PREFIX", proximityIndexPrefixEnv, "Префикс для таргетного индекса.")

	proximityAmbitEnv, _ := strconv.Atoi(helpers.Env("PROXIMITY_AMBIT", "15"))
	flag.IntVar(&proximityAmbit, "PROXIMITY_AMBIT", proximityAmbitEnv, "Размерность окрестности.")

	keepAliveEnv, _ := strconv.Atoi(helpers.Env("SCROLL_KEEP_ALIVE", "5"))
	flag.IntVar(&keepAlive, "SCROLL_KEEP_ALIVE", keepAliveEnv, "Срок жизни токена для Scroll API в минутах.")

	pageSizeEnv, _ := strconv.Atoi(helpers.Env("SINGLE_PAGE_SIZE", "1000"))
	flag.IntVar(&pageSize, "SINGLE_PAGE_SIZE", pageSizeEnv, "Размер одной страницы для Scroll API. Данный параметр влияет на потребление CPU!")

	uploadChunkSizeEnv, _ := strconv.Atoi(helpers.Env("UPLOAD_CHUNK_SIZE", "1000000"))
	flag.IntVar(&uploadChunkSize, "UPLOAD_CHUNK_SIZE", uploadChunkSizeEnv, "Размерность буффера для хранения готовых для отправки окрестностей. Данный параметр влияет на потребление ОЗУ!")

	flag.BoolVar(&LoggerEnable, "ELASTIC_DEBUG_REQUESTS", false, "Параметр для активации логгера для каждого отдельного запроса в Elasticsearch.")

	flag.Parse()

	if Username != "" && Password == "" {
		logger.Error("Указан пользователь, но не указан пароль. Используйте -ELASTIC_PASSWORD=...")
	}

	if Username == "" && Password != "" {
		logger.Error("Указан пароль, но не указан пользователь. Используйте -ELASTIC_USERNAME=...")
	}

	if sourceIndex == "" {
		logger.Error("Не указан индекс источник. Используйте -SOURCE_INDEX=...")
	}

	if sourceIndex == "" {
		logger.Error("Не указан префикс для таргетного индекса. Используйте -TARGET_INDEX_PREFIX=...")
	}
}

func init() {
	startTime := time.Now()

	initWithFlags()

	config = calculator.Config{
		Elastic: elastic.Config{
			Scheme:       Scheme,
			Address:      Address,
			Port:         Port,
			Username:     Username,
			Password:     Password,
			LoggerEnable: LoggerEnable,
		},
		ProximityAmbit:       proximityAmbit,
		KeepAlive:            keepAlive,
		SourceIndex:          sourceIndex,
		ProximityIndexPrefix: proximityIndexPrefix,
		PageSize:             pageSize,
		UploadChunkSize:      uploadChunkSize,
		Start:                startTime,
	}

	if Username != "" && Password != "" {
		logger.Info(
			fmt.Sprintf(
				"---- Параметры:\n\nElasitcsearch: %s://%s:%s [username: %s, password: %s]\nРазмерность окрестности: %d\nВремя жизни токена Scroll API (в минутах): %d\nИндекс источник: %s\nПрефикс таргетного индекса: %s\nРазмер одной страницы для Scroll API: %d\nРазмерность буффера для хранения готовых для отправки окрестностей: %d\n",
				Scheme,
				Address,
				Port,
				Username,
				Password,
				proximityAmbit,
				keepAlive,
				sourceIndex,
				proximityIndexPrefix,
				pageSize,
				uploadChunkSize,
			),
		)
	} else {
		logger.Info(
			fmt.Sprintf(
				"---- Параметры:\n\nElasitcsearch: %s://%s:%s\nРазмерность окрестности: %d\nВремя жизни токена Scroll API (в минутах): %d\nИндекс источник: %s\nПрефикс таргетного индекса: %s\nРазмер одной страницы для Scroll API: %d\nРазмерность буффера для хранения готовых для отправки окрестностей: %d\n",
				Scheme,
				Address,
				Port,
				proximityAmbit,
				keepAlive,
				sourceIndex,
				proximityIndexPrefix,
				pageSize,
				uploadChunkSize,
			),
		)
	}
}

func main() {
	logger.Info("Начало работы")

	calculator.Do(config)

	logger.Info("Конец работы")
}
