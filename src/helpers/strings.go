package helpers

import (
	"bytes"
	"io"
	"math/rand"
	"time"
	"unsafe"
)

// ReaderToString Функция для конвертации io.Reader в string
func ReaderToString(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

func RandomString(n int) string {
	var src = rand.NewSource(time.Now().UnixNano())
	const (
		letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		letterIdxBits = 6
		letterIdxMask = 1<<letterIdxBits - 1
		letterIdxMax  = 63 / letterIdxBits
	)

	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
