package memtable

import (
	"fmt"
	"testing"

	"github.com/aixiasang/sqldb/utils"
)

func TestBtreeIterator(t *testing.T) {
	bt := NewBTreeMemTable(32)
	for i := 0; i < 100; i++ {
		key, value := utils.GenerateKey(i), utils.GenerateValue(i)
		bt.Put(key, value)
	}
	iter := bt.Iterator()
	iter.First()
	for iter.Next() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
	}
}
