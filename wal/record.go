package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// RecordType 记录类型
type RecordType uint8

const (
	RecordTypePut    RecordType = iota // 写入
	RecordTypeDelete                   // 删除
)

// Record 记录
type Record struct {
	RecordType RecordType // 记录类型
	Key        []byte     // 键
	Value      []byte     // 值
}

func NewRecord(key, value []byte) *Record {
	if value == nil {
		return newRecord(key, nil, RecordTypeDelete)
	}
	return newRecord(key, value, RecordTypePut)
}

func newRecord(key, value []byte, recordType RecordType) *Record {
	return &Record{
		Key:        key,
		Value:      value,
		RecordType: recordType,
	}
}
func (r *Record) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(byte(r.RecordType)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(r.Key))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(r.Value))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(r.Key); err != nil {
		return nil, err
	}
	if _, err := buf.Write(r.Value); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < 9 { // 至少需要 1 字节类型 + 4 字节 key 长度 + 4 字节 value 长度
		return nil, errors.New("record data too short")
	}

	recordType := RecordType(data[0])

	var keyLength uint32
	if err := binary.Read(bytes.NewReader(data[1:5]), binary.BigEndian, &keyLength); err != nil {
		return nil, err
	}

	var valueLength uint32
	if err := binary.Read(bytes.NewReader(data[5:9]), binary.BigEndian, &valueLength); err != nil {
		return nil, err
	}

	// 验证长度合理性
	if keyLength > 10*1024*1024 || valueLength > 100*1024*1024 {
		return nil, fmt.Errorf("key or value length too large: keyLength=%d, valueLength=%d", keyLength, valueLength)
	}

	// 验证数据长度是否足够
	expectedLength := 9 + keyLength + valueLength + 4 // header + key + value + crc
	if uint32(len(data)) < expectedLength {
		return nil, errors.New("record data incomplete")
	}

	// 读取 key 和 value
	key := data[9 : 9+keyLength]
	value := data[9+keyLength : 9+keyLength+valueLength]

	// 验证 CRC
	crcData := data[9+keyLength+valueLength:]
	var storedCrc uint32
	if err := binary.Read(bytes.NewReader(crcData), binary.BigEndian, &storedCrc); err != nil {
		return nil, err
	}

	// 计算 CRC
	actualCrc := crc32.ChecksumIEEE(data[:9+keyLength+valueLength])
	if storedCrc != actualCrc {
		return nil, errors.New("crc mismatch")
	}

	return &Record{
		RecordType: recordType,
		Key:        key,
		Value:      value,
	}, nil
}
func DecodeStream(r io.Reader, callback func(key, value []byte) error) error {
	offset := 0
	buf := make([]byte, 1+4+4)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || n == 0 {
				return nil
			}
			return err
		}
		rec := &Record{}
		// todo: 这里需要优化 recordType 的读取
		rec.RecordType = RecordType(buf[0])
		var keyLength uint32
		if err := binary.Read(bytes.NewBuffer(buf[1:5]), binary.BigEndian, &keyLength); err != nil {
			return err
		}
		var valueLength uint32
		if err := binary.Read(bytes.NewBuffer(buf[5:9]), binary.BigEndian, &valueLength); err != nil {
			return err
		}
		key := make([]byte, keyLength)
		if _, err := io.ReadFull(bytes.NewBuffer(buf[9:9+keyLength]), key); err != nil {
			return err
		}
		value := make([]byte, valueLength)
		if _, err := io.ReadFull(bytes.NewBuffer(buf[9+keyLength:]), value); err != nil {
			return err
		}
		var expectCrc uint32
		if err := binary.Read(bytes.NewBuffer(buf[9+keyLength+valueLength:]), binary.BigEndian, &expectCrc); err != nil {
			return err
		}
		fullData := append(buf[:], key...)
		fullData = append(fullData, value...)
		crc := crc32.ChecksumIEEE(fullData)
		if crc != expectCrc {
			return errors.New("crc mismatch")
		}
		if err := callback(key, value); err != nil {
			return err
		}
		offset += n
	}
}
