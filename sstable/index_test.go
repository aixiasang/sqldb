package sstable

import (
	"bytes"
	"testing"
)

func TestIndex_Encode(t *testing.T) {
	index := &Index{
		minKey: []byte("a"),
		maxKey: []byte("b"),
		offset: 1,
		length: 2,
	}
	data, err := index.Encode()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data)
	index2, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(index2)
}
func TestDecodeStream(t *testing.T) {
	indexes := []*Index{
		{
			minKey: []byte("a"),
			maxKey: []byte("b"),
			offset: 1,
			length: 2,
		},
		{
			minKey: []byte("c"),
			maxKey: []byte("d"),
			offset: 3,
			length: 4,
		},
	}
	buf := new(bytes.Buffer)
	for _, index := range indexes {
		data, err := index.Encode()
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(data)
	}
	indexes2, err := DecodeStream(buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(indexes2)
}
