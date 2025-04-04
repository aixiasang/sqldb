package sstable

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/filter"
	"github.com/aixiasang/sqldb/utils"
)

type DataBlock struct {
	conf    *config.Config // 配置
	dataBuf *bytes.Buffer  // 数据缓冲区
	minKey  []byte         // 最小key
	maxKey  []byte         // 最大key
	mu      *sync.RWMutex  // 互斥锁
}

func NewDataBlock(conf *config.Config) *DataBlock {
	return &DataBlock{
		conf:    conf,
		dataBuf: bytes.NewBuffer(nil),
		minKey:  []byte{},
		maxKey:  []byte{},
		mu:      &sync.RWMutex{},
	}

}

func (d *DataBlock) Add(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.minKey) == 0 {
		d.minKey = utils.CopyKey(key)
	}
	d.maxKey = utils.CopyKey(key)

	if err := binary.Write(d.dataBuf, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(d.dataBuf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := d.dataBuf.Write(key); err != nil {
		return err
	}
	if _, err := d.dataBuf.Write(value); err != nil {
		return err
	}
	return nil
}
func (d *DataBlock) Bytes() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dataBuf.Bytes()
}

func (d *DataBlock) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dataBuf.Len()
}
func (d *DataBlock) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dataBuf.Reset()
	d.minKey = nil
	d.maxKey = nil
}
func (d *DataBlock) MinKey() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return utils.CopyKey(d.minKey)
}
func (d *DataBlock) MaxKey() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return utils.CopyKey(d.maxKey)
}

type BloomBlock struct {
	conf     *config.Config // 配置
	filter   filter.Filter  // 布隆过滤器
	bloomBuf *bytes.Buffer  // 布隆过滤器缓冲区
	mu       *sync.RWMutex  // 互斥锁
}

func NewBloomBlock(conf *config.Config) *BloomBlock {
	return &BloomBlock{
		conf:     conf,
		filter:   config.NewFilterConstructor(),
		bloomBuf: bytes.NewBuffer(nil),
		mu:       &sync.RWMutex{},
	}
}
func (b *BloomBlock) Add(offset uint64, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := binary.Write(b.bloomBuf, binary.BigEndian, offset); err != nil {
		return err
	}
	if err := binary.Write(b.bloomBuf, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := b.bloomBuf.Write(data); err != nil {
		return err
	}
	return nil
}
func (b *BloomBlock) Bytes() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bloomBuf.Bytes()
}
func (b *BloomBlock) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bloomBuf.Len()
}
func (b *BloomBlock) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bloomBuf.Reset()
	b.filter.Reset()
}
