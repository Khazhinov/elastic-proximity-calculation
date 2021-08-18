package logger

import (
	"elastic-proximity-calculation/src/helpers"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

var logger *logrus.Logger
var isInit bool = false
var Hash = helpers.RandomString(10)

func Info(args ...string) {
	if !isInit {
		InitLogger()
	}

	if len(args) > 1 {
		logger.WithField("hash", Hash).Infoln(fmt.Sprintf(args[0], args[1]))
		fmt.Println(getCurrentTimeString() + "[INFO] " + fmt.Sprintf(args[0], args[1]))
	} else {
		logger.WithField("hash", Hash).Infoln(args[0])
		fmt.Println(getCurrentTimeString() + "[INFO] " + args[0])
	}
}

func Warning(args ...string) {
	if !isInit {
		InitLogger()
	}

	if len(args) > 1 {
		logger.WithField("hash", Hash).Warningln(fmt.Sprintf(args[0], args[1]))
		fmt.Println(getCurrentTimeString() + "[WARNING] " + fmt.Sprintf(args[0], args[1]))
	} else {
		logger.WithField("hash", Hash).Warningln(args[0])
		fmt.Println(getCurrentTimeString() + "[WARNING] " + args[0])
	}
}

func Error(args ...string) {
	if !isInit {
		InitLogger()
	}

	if len(args) > 1 {
		logger.WithField("hash", Hash).Errorln(fmt.Sprintf(args[0], args[1]))
		fmt.Println(getCurrentTimeString() + "[ERROR] " + fmt.Sprintf(args[0], args[1]))
		os.Exit(1)
	} else {
		logger.WithField("hash", Hash).Errorln(args[0])
		fmt.Println(getCurrentTimeString() + "[ERROR] " + args[0])
		os.Exit(1)
	}
}

func InitLogger() {
	logger = logrus.New()

	var logFileName strings.Builder
	logFileName.WriteString("result")
	logFileName.WriteString(time.Now().Format("-2006-01-02"))
	logFileName.WriteString(".log")

	file, err := os.OpenFile(logFileName.String(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		logger.Out = file
	} else {
		logger.Info("Не удалось открыть файл " + logFileName.String() + " для записи логов. Используется stderr/stdout")
	}

	logger.SetFormatter(&logrus.JSONFormatter{})

	isInit = true

	var initMessage strings.Builder
	initMessage.WriteString("Уникальных хэш сессии: ")
	initMessage.WriteString(Hash)

	Info(initMessage.String())
}

func getCurrentTimeString() string {
	return time.Now().Format("[2006-01-02 15:04:05] ")
}
