package utils

import (
	"fmt"
)

// GenerateKey 生成测试用的键
func GenerateKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%03d", i))
}

// GenerateValue 生成测试用的值
func GenerateValue(i int) []byte {
	return []byte(fmt.Sprintf("value-%03d", i))
}
