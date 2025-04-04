package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Index struct {
	minKey []byte // 最小key
	maxKey []byte // 最大key
	offset uint64 // 偏移量
	length uint64 // 长度
}

func (i *Index) String() string {
	return fmt.Sprintf("Index{minKey: %s, maxKey: %s, offset: %d, length: %d}", i.minKey, i.maxKey, i.offset, i.length)
}
func (i *Index) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.minKey))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.maxKey))); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, i.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, i.length); err != nil {
		return nil, err
	}
	if _, err := buf.Write(i.minKey); err != nil {
		return nil, err
	}
	if _, err := buf.Write(i.maxKey); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
