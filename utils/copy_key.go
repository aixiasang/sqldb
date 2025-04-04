package utils

// 安全复制key
func CopyKey(key []byte) []byte {
	copyKey := make([]byte, len(key))
	copy(copyKey, key)
	return copyKey
}
