package config

import (
	"github.com/aixiasang/sqldb/filter"
	"github.com/aixiasang/sqldb/memtable"
)

const (
	DefaultBlockSize            = 4096
	DefaultBloomFilterSize      = 1024
	DefaultBloomFilterHashCount = 3
	DefaultMemTableType         = memtable.MemTableTypeBTree
	DefaultWalDir               = "wal"
	DefaultSSTDir               = "sst"
	DefaultAutoSync             = false
	DefaultIsDebug              = false
	DefaultMemTableCapSize      = 4096
	DefaultMaxLevel             = 7
)

type Config struct {
	DataDir         string // 数据目录
	BlockSize       int64  // 块大小
	WalDir          string // WAL目录
	AutoSync        bool   // 自动同步
	IsDebug         bool   // 是否开启调试
	MemTableCapSize int64  // 内存表容量
	SSTDir          string // SST目录
	MaxLevel        int    // LSM树最大层级数
}

func NewConfig() *Config {
	return &Config{
		DataDir:         "./data",
		BlockSize:       4096 / 4,
		WalDir:          DefaultWalDir,
		AutoSync:        DefaultAutoSync,
		IsDebug:         DefaultIsDebug,
		MemTableCapSize: DefaultMemTableCapSize,
		SSTDir:          DefaultSSTDir,
		MaxLevel:        DefaultMaxLevel,
	}
}
func NewMemTableConstructor() memtable.MemTable {
	return memtable.NewMemTable(DefaultMemTableType, DefaultMemTableCapSize)
}
func NewFilterConstructor() filter.Filter {
	return filter.NewBloomFilter(DefaultBloomFilterSize, DefaultBloomFilterHashCount)
}
