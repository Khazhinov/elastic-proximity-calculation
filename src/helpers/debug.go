package helpers

import (
	"fmt"
	"os"
)

// DD Функция для дебага.
// Принимает в себя любое количество аргументов и печатает их в консоль, послу чего завершает выполнение программы.
func DD(args ...interface{}) {
	fmt.Printf("%#v\n", args)
	os.Exit(0)
}
