package sstable

import (
	"fmt"
	"testing"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/utils"
)

func TestSSTWriter_Write(t *testing.T) {
	conf := config.NewConfig()
	conf.BlockSize = 256
	writer, err := NewSSTWriter("test.sst", conf)
	if err != nil {
		t.Fatal(err)
	}
	mt := config.NewMemTableConstructor()
	d := make(map[string]string)
	for i := range 100 {
		key := utils.GenerateKey(i)
		value := utils.GenerateValue(i)
		mt.Put(key, value)
		d[string(key)] = string(value)
	}
	if err := writer.Write(mt); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestSSTReader_Get(t *testing.T) {
	conf := config.NewConfig()
	conf.BlockSize = 256
	reader, err := NewSSTReader("test.sst", conf)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := reader.Get(utils.GenerateKey(1))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("key not found")
	}
	if string(value) != "value_1" {
		t.Fatal("value not match")
	}
	fmt.Println("value", value)
}
