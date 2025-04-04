package filter

import "math"

// Filter 过滤器接口
type Filter interface {
	Add(key []byte)           // 添加key
	Contains(key []byte) bool // 判断key是否存在
	Save() []byte             // 保存到文件
	Load(data []byte) error   // 从文件加载
	Reset()                   // 重置
}

// NewBloomFilter 创建一个新的布隆过滤器
func NewBloomFilter(m uint64, k uint) Filter {
	if m == 0 {
		m = 1024 // 默认大小
	}
	if k == 0 {
		k = 3 // 默认哈希函数数量
	}

	// 确保使用默认种子并且数量足够
	seeds := make([]uint32, k)
	for i := uint(0); i < k; i++ {
		if i < uint(len(defaultSeeds)) {
			seeds[i] = defaultSeeds[i]
		} else {
			// 如果默认种子不足，使用默认种子的组合
			seeds[i] = defaultSeeds[i%uint(len(defaultSeeds))] + uint32(i/uint(len(defaultSeeds))*7)
		}
	}

	return &BloomFilter{
		m:     m,
		k:     k,
		bits:  make([]uint64, (m+63)/64), // 向上取整到64的倍数
		n:     0,
		seeds: seeds,
	}
}

// NewBloomFilterWithParams 使用预期元素数量和误判率创建布隆过滤器
func NewBloomFilterWithParams(expectedElements uint64, falsePositiveRate float64) Filter {
	// 计算最佳的m和k值
	// m = -n*ln(p)/(ln(2)^2)
	m := uint64(math.Ceil(-float64(expectedElements) * math.Log(falsePositiveRate) / math.Pow(math.Log(2), 2)))
	// k = m/n * ln(2)
	k := uint(math.Ceil(float64(m) / float64(expectedElements) * math.Log(2)))

	// 确保k至少为1
	if k < 1 {
		k = 1
	}

	return NewBloomFilter(m, k)
}
