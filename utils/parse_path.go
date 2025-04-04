package utils

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func ParseSSTPath(path string) (int, uint32, error) {
	// 获取文件名 data/sst/0_1.sst
	fileName := filepath.Base(path)
	// 获取文件名中的level和seq
	str := strings.TrimSuffix(fileName, ".sst")
	parts := strings.Split(str, "_")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid path: %s", path)
	}
	level, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid path: %s", path)
	}
	seq, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid path: %s", path)
	}
	return level, uint32(seq), nil
}
func ParseWalPath(path string) (uint32, error) {
	fileName := filepath.Base(path)
	parts := strings.Split(fileName, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid path: %s", path)
	}
	fileId, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid path: %s", path)
	}
	return uint32(fileId), nil
}
