package helpers

import (
	"github.com/joho/godotenv"
	"os"
)

// Env Функция для получения переменной из .env файла
// Позволяет передавать вторым аргументом значение по умолчанию
func Env(name string, defaultVal ...string) string {
	err := godotenv.Load(".env")

	if err != nil {
		if len(defaultVal) > 0 {
			return defaultVal[0]
		} else {
			return ""
		}
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
