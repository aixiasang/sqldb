package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
)

func Decode(data []byte) (*Index, error) {
	buf := bytes.NewBuffer(data)
	var minKeyLen, maxKeyLen uint32
	if err := binary.Read(buf, binary.BigEndian, &minKeyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &maxKeyLen); err != nil {
		return nil, err
	}

	var offset, length uint64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	minKey := buf.Next(int(minKeyLen))
	maxKey := buf.Next(int(maxKeyLen))
	return &Index{minKey: minKey, maxKey: maxKey, offset: offset, length: length}, nil
}
func DecodeStream(r io.Reader) ([]*Index, error) {
	//读取index
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	indexes := make([]*Index, 0)
	for buf.Len() > 0 {
		// 读取key
		minKeyLen := binary.BigEndian.Uint32(buf.Next(4))
		maxKeyLen := binary.BigEndian.Uint32(buf.Next(4))
		offset := binary.BigEndian.Uint64(buf.Next(8))
		length := binary.BigEndian.Uint64(buf.Next(8))
		minKey := buf.Next(int(minKeyLen))
		maxKey := buf.Next(int(maxKeyLen))
		indexes = append(indexes, &Index{minKey: minKey, maxKey: maxKey, offset: offset, length: length})
	}
	return indexes, nil
}
