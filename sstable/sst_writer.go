package sstable

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/filter"
	"github.com/aixiasang/sqldb/memtable"
)

type SSTWriter struct {
	filename    string         // 文件名
	dest        *os.File       // 文件描述符
	dataBuf     *bytes.Buffer  // 数据缓冲区
	indexBuf    *bytes.Buffer  // 索引缓冲区
	filterBuf   *bytes.Buffer  // 过滤器缓冲区
	block       *DataBlock     // 数据块
	filterBlock *BloomBlock    // 过滤器块
	filter      filter.Filter  // 过滤器
	conf        *config.Config // 配置
	indexs      []*Index       // 索引
}

func NewSSTWriter(filename string, conf *config.Config) (*SSTWriter, error) {
	dest, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &SSTWriter{
		filename:    filename,
		dest:        dest,
		dataBuf:     bytes.NewBuffer(nil),
		indexBuf:    bytes.NewBuffer(nil),
		block:       NewDataBlock(conf),
		filterBlock: NewBloomBlock(conf),
		conf:        conf,
		filter:      config.NewFilterConstructor(),
		filterBuf:   bytes.NewBuffer(nil),
	}, nil
}

func (w *SSTWriter) Write(mem memtable.MemTable) error {
	iter := mem.Iterator()
	for iter.Next() {
		if err := w.writeKV(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	if w.block.Size() > 0 {
		if err := w.mustFlush(); err != nil {
			return err
		}
	}
	dataLength := w.dataBuf.Len()
	for _, index := range w.indexs {
		buf, err := index.Encode()
		if err != nil {
			return err
		}
		if _, err := w.indexBuf.Write(buf); err != nil {
			return err
		}
	}
	indexLength := w.indexBuf.Len()
	if _, err := w.dest.Write(w.dataBuf.Bytes()); err != nil {
		return err
	}
	if _, err := w.dest.Write(w.indexBuf.Bytes()); err != nil {
		return err
	}
	filterLength := w.filterBlock.Size()
	if _, err := w.dest.Write(w.filterBlock.Bytes()); err != nil {
		return err
	}
	if err := binary.Write(w.dest, binary.BigEndian, uint64(dataLength)); err != nil {
		return err
	}
	if err := binary.Write(w.dest, binary.BigEndian, uint64(indexLength)); err != nil {
		return err
	}
	if err := binary.Write(w.dest, binary.BigEndian, uint64(filterLength)); err != nil {
		return err
	}
	// fmt.Println("dataLength", dataLength)
	// fmt.Println("indexLength", indexLength)
	// fmt.Println("filterLength", filterLength)
	// fmt.Println("indexs", w.indexs)
	return nil
}
func (w *SSTWriter) writeKV(key, value []byte) error {
	w.filter.Add(key)
	if err := w.block.Add(key, value); err != nil {
		return err
	}
	if err := w.tryFlush(); err != nil {
		return err
	}
	return nil
}
func (w *SSTWriter) Flush() error {
	return nil
}

func (w *SSTWriter) tryFlush() error {
	if w.block.Size() > int(w.conf.BlockSize) {
		return w.mustFlush()
	}
	return nil
}
func (w *SSTWriter) mustFlush() error {
	minKey := w.block.MinKey()
	maxKey := w.block.MaxKey()
	offset := w.dataBuf.Len()

	length, err := w.dataBuf.Write(w.block.Bytes())
	if err != nil {
		return err
	}
	w.block.Clear()
	w.indexs = append(w.indexs, &Index{
		minKey: minKey,
		maxKey: maxKey,
		offset: uint64(offset),
		length: uint64(length),
	})
	if err := w.filterBlock.Add(uint64(offset), w.filter.Save()); err != nil {
		return err
	}
	w.filter.Reset()
	return nil
}
func (w *SSTWriter) Close() error {
	return w.dest.Close()
}
