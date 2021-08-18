package helpers

import (
	"math"
	"strconv"
)

// StringToFloat64 Функция для конвертирования строки в float64
func StringToFloat64(stringNumber string) float64 {
	num, _ := strconv.ParseFloat(stringNumber, 64)
	num = Round(num, 5)

	return num
}

// Round функция для округления float64 до определенного знака после запятой
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

// IsNumber функция для проверки наличия ТОЛЬКО числа в строке.
// Предполагает, что число не является math.NaN, (+-)math.Inf и -0
func IsNumber(token string) bool {
	if num, err := strconv.ParseFloat(token, 64); err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) && !math.IsInf(num, -1) && !math.IsInf(num, 1) && num != -0 {
		return true
	}

	return false
}
