package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/filter"
	"github.com/aixiasang/sqldb/utils"
)

const (
	footerLength       = 24
	filterHeaderLength = 8 + 4
	indexHeaderLength  = 4 + 4 + 8 + 8
)

type SSTReader struct {
	filename     string            // 文件名
	mu           *sync.RWMutex     // 互斥锁
	src          *os.File          // 文件描述符
	conf         *config.Config    // 配置
	indexs       []*Index          // 索引
	dataLength   uint64            // 数据长度
	indexLength  uint64            // 索引长度
	filterLength uint64            // 过滤器长度
	filterMap    map[uint64][]byte // 过滤器映射
	bloomFilter  filter.Filter     // 布隆过滤器
}

func NewSSTReader(filename string, conf *config.Config) (*SSTReader, error) {
	src, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	reader := &SSTReader{
		filename:    filename,
		src:         src,
		conf:        conf,
		filterMap:   make(map[uint64][]byte),
		mu:          &sync.RWMutex{},
		bloomFilter: config.NewFilterConstructor(),
	}
	if err := reader.readFooter(); err != nil {
		return nil, err
	}
	if err := reader.readIndex(); err != nil {
		return nil, err
	}
	if err := reader.readFilter(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *SSTReader) readFooter() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	footer := make([]byte, footerLength)
	fileInfo, err := r.src.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if _, err := r.src.ReadAt(footer, fileSize-24); err != nil {
		return err
	}
	var dataLength, indexLength, filterLength uint64
	if err := binary.Read(bytes.NewBuffer(footer[:8]), binary.BigEndian, &dataLength); err != nil {
		return err
	}
	if err := binary.Read(bytes.NewBuffer(footer[8:16]), binary.BigEndian, &indexLength); err != nil {
		return err
	}
	if err := binary.Read(bytes.NewBuffer(footer[16:]), binary.BigEndian, &filterLength); err != nil {
		return err
	}
	r.dataLength = dataLength
	r.indexLength = indexLength
	r.filterLength = filterLength
	return nil
}
func (r *SSTReader) readIndex() error {
	return r.readIndexData(r.dataLength, r.indexLength)
}
func (r *SSTReader) readIndexData(offset uint64, length uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.src.Seek(int64(offset), io.SeekStart)
	var currOffset uint64 = 0
	var indexs []*Index
	for currOffset < length {
		buf := make([]byte, indexHeaderLength)
		if _, err := r.src.Read(buf); err != nil {
			return err
		}
		minKeyLen := binary.BigEndian.Uint32(buf[:4])
		maxKeyLen := binary.BigEndian.Uint32(buf[4:8])
		offset := binary.BigEndian.Uint64(buf[8:16])
		length := binary.BigEndian.Uint64(buf[16:24])
		reminderData := make([]byte, minKeyLen+maxKeyLen)
		if _, err := r.src.Read(reminderData); err != nil {
			return err
		}
		minKey := reminderData[:minKeyLen]
		maxKey := reminderData[minKeyLen : minKeyLen+maxKeyLen]
		index := &Index{minKey: minKey, maxKey: maxKey, offset: offset, length: length}
		indexs = append(indexs, index)
		currOffset += indexHeaderLength + uint64(minKeyLen) + uint64(maxKeyLen)
	}
	r.indexs = indexs
	return nil
}

func (r *SSTReader) readData(offset uint64, length uint64, target []byte) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.src.Seek(int64(offset), io.SeekStart)
	var currOffset uint64 = 0

	for currOffset < length {
		var keyLen, valueLen uint32
		var key, value []byte
		if err := binary.Read(r.src, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				return nil, false, nil
			}
			return nil, false, err
		}
		if err := binary.Read(r.src, binary.BigEndian, &valueLen); err != nil {
			return nil, false, err
		}
		key = make([]byte, keyLen)
		value = make([]byte, valueLen)
		if _, err := r.src.Read(key); err != nil {
			return nil, false, err
		}
		if _, err := r.src.Read(value); err != nil {
			return nil, false, err
		}
		if utils.CompareBytes(key, target) == 0 {
			return value, true, nil
		}
		currOffset += uint64(keyLen) + uint64(valueLen)
	}
	return nil, false, nil
}
func (r *SSTReader) readFilter() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.src.Seek(int64(r.dataLength+r.indexLength), io.SeekStart)
	var currOffset uint64 = 0
	for currOffset < r.filterLength {
		var offset uint64
		var length uint32
		if err := binary.Read(r.src, binary.BigEndian, &offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := binary.Read(r.src, binary.BigEndian, &length); err != nil {
			return err
		}
		filterData := make([]byte, length)
		if _, err := r.src.Read(filterData); err != nil {
			return err
		}
		r.filterMap[offset] = filterData
		currOffset += uint64(length) + filterHeaderLength
	}
	return nil
}
func (r *SSTReader) getIndex(key []byte) (*Index, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, index := range r.indexs {
		if bytes.Compare(key, index.minKey) >= 0 && bytes.Compare(key, index.maxKey) <= 0 {
			return index, nil
		}
	}
	return nil, errors.New("index not found")
}
func (r *SSTReader) Get(key []byte) ([]byte, bool, error) {
	index, err := r.getIndex(key)
	if err != nil {
		return nil, false, err
	}
	if data, ok := r.filterMap[index.offset]; ok {
		r.bloomFilter.Load(data)
		if !r.bloomFilter.Contains(key) {
			return nil, false, nil
		}
		return r.readData(index.offset, index.length, key)
	}
	return nil, false, errors.New("filter not found")
}
func (r *SSTReader) readDataCallback(offset uint64, length uint64, callback func([]byte, []byte) bool) error {
	r.src.Seek(int64(offset), io.SeekStart)
	var currOffset uint64 = 0

	for currOffset < length {
		var keyLen, valueLen uint32
		var key, value []byte
		if err := binary.Read(r.src, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := binary.Read(r.src, binary.BigEndian, &valueLen); err != nil {
			return err
		}
		key = make([]byte, keyLen)
		value = make([]byte, valueLen)
		if _, err := r.src.Read(key); err != nil {
			return err
		}
		if _, err := r.src.Read(value); err != nil {
			return err
		}
		if callback(key, value) {
			return nil
		}
		currOffset += uint64(keyLen) + uint64(valueLen)
	}
	return nil
}
func (r *SSTReader) Close() error {
	_ = r.src.Close()
	r.src = nil
	r.indexs = nil
	r.filterMap = nil
	r.dataLength = 0
	r.indexLength = 0
	r.filterLength = 0
	r.conf = nil
	r.filename = ""
	return nil
}

// Iterator interface for iterating over key-value pairs in an SSTable
type Iterator interface {
	// First positions the iterator at the first key
	First()
	// Next advances the iterator to the next key, returns false if no more keys
	Next() bool
	// Valid returns whether the iterator is positioned at a valid key
	Valid() bool
	// Key returns the current key the iterator is positioned at
	Key() []byte
	// Value returns the current value the iterator is positioned at
	Value() []byte
}

// SSTIterator implements the Iterator interface for SSTReader
type SSTIterator struct {
	reader     *SSTReader
	index      int    // Current index position
	indexCount int    // Total number of indexes
	dataOffset uint64 // Current data offset
	dataLength uint64 // Current data length
	currKey    []byte // Current key
	currValue  []byte // Current value
	valid      bool   // Whether current position is valid
	currOffset uint64 // Current offset within data block
}

// Iterator returns a new iterator for the SSTable
func (r *SSTReader) Iterator() Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	indexCount := len(r.indexs)
	iter := &SSTIterator{
		reader:     r,
		index:      -1,
		indexCount: indexCount,
		valid:      false,
	}

	return iter
}

// First positions the iterator at the first key-value pair
func (it *SSTIterator) First() {
	if it.indexCount == 0 {
		it.valid = false
		return
	}

	it.index = 0
	it.loadCurrentIndex()
	it.valid = it.readNextKeyValue()
}

// loadCurrentIndex loads the current index information
func (it *SSTIterator) loadCurrentIndex() {
	index := it.reader.indexs[it.index]
	it.dataOffset = index.offset
	it.dataLength = index.length
	it.currOffset = 0
}

// Next advances to the next key-value pair
func (it *SSTIterator) Next() bool {
	if !it.valid {
		return false
	}

	// Try to read next key-value in current index
	if it.readNextKeyValue() {
		return true
	}

	// Move to next index
	it.index++
	if it.index >= it.indexCount {
		it.valid = false
		return false
	}

	it.loadCurrentIndex()
	it.valid = it.readNextKeyValue()
	return it.valid
}

// readNextKeyValue reads the next key-value pair in the current index
func (it *SSTIterator) readNextKeyValue() bool {
	if it.currOffset >= it.dataLength {
		return false
	}

	reader := it.reader
	reader.mu.RLock()
	defer reader.mu.RUnlock()

	// Seek to the current position in the data block
	_, err := reader.src.Seek(int64(it.dataOffset+it.currOffset), io.SeekStart)
	if err != nil {
		return false
	}

	// Read key and value lengths
	var keyLen, valueLen uint32
	if err := binary.Read(reader.src, binary.BigEndian, &keyLen); err != nil {
		return false
	}
	if err := binary.Read(reader.src, binary.BigEndian, &valueLen); err != nil {
		return false
	}

	// Read key and value
	key := make([]byte, keyLen)
	value := make([]byte, valueLen)
	if _, err := reader.src.Read(key); err != nil {
		return false
	}
	if _, err := reader.src.Read(value); err != nil {
		return false
	}

	// Update current key and value
	it.currKey = key
	it.currValue = value

	// Update offset
	it.currOffset += 8 + uint64(keyLen) + uint64(valueLen) // 8 bytes for key and value lengths

	return true
}

// Valid returns whether the iterator is positioned at a valid key-value pair
func (it *SSTIterator) Valid() bool {
	return it.valid
}

// Key returns the current key
func (it *SSTIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.currKey
}

// Value returns the current value
func (it *SSTIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.currValue
}
