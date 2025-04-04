package memtable

import (
	"errors"
	"sync"

	"github.com/aixiasang/sqldb/utils"
	"github.com/google/btree"
)

// KVItem 用于存储在B树中的键值对
type KVItem struct {
	key   []byte
	value []byte
}

// Less 实现btree.Item接口的Less方法
func (i *KVItem) Less(than btree.Item) bool {
	// return bytes.Compare(i.key, than.(*KVItem).key) < 0
	return utils.CompareBytes(i.key, than.(*KVItem).key) < 0
}

// BTreeMemTable B树内存表实现
type BTreeMemTable struct {
	tree  *btree.BTree
	mutex sync.RWMutex // 读写锁，用于并发控制
}

// Iterator implements MemTable.
func (bt *BTreeMemTable) Iterator() Iterator {
	return newBtreeIterator(bt)
}

// NewBTreeMemTable 创建一个新的B树内存表
func NewBTreeMemTable(degree int) *BTreeMemTable {
	if degree <= 0 {
		degree = 2 // 默认度为2
	}
	return &BTreeMemTable{
		tree: btree.New(degree),
	}
}

// Put 向B树中插入键值对
func (bt *BTreeMemTable) Put(key, value []byte) error {
	if key == nil {
		return errors.New("key is nil")
	}

	item := &KVItem{
		key:   append([]byte{}, key...),   // 深拷贝，避免外部修改
		value: append([]byte{}, value...), // 深拷贝，避免外部修改
	}

	bt.mutex.Lock()         // 写操作加锁
	defer bt.mutex.Unlock() // 确保操作完成后解锁

	bt.tree.ReplaceOrInsert(item)
	return nil
}

// Get 从B树中获取值
func (bt *BTreeMemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("key is nil")
	}

	searchItem := &KVItem{key: key}

	bt.mutex.RLock()         // 读操作加读锁
	defer bt.mutex.RUnlock() // 确保操作完成后解锁

	item := bt.tree.Get(searchItem)
	if item == nil {
		return nil, errors.New("key not found")
	}

	kvItem := item.(*KVItem)
	return append([]byte{}, kvItem.value...), nil // 返回拷贝，避免外部修改
}

// Delete 从B树中删除一个键值对
func (bt *BTreeMemTable) Delete(key []byte) error {
	if key == nil {
		return errors.New("key is nil")
	}

	searchItem := &KVItem{key: key}

	bt.mutex.Lock()         // 写操作加锁
	defer bt.mutex.Unlock() // 确保操作完成后解锁

	item := bt.tree.Delete(searchItem)
	if item == nil {
		return errors.New("key not found")
	}

	return nil
}

// ForEach 遍历B树中的所有键值对
func (bt *BTreeMemTable) ForEach(visitor func(key, value []byte) bool) {
	bt.mutex.RLock()         // 读操作加读锁
	defer bt.mutex.RUnlock() // 确保操作完成后解锁

	bt.tree.Ascend(func(i btree.Item) bool {
		kvItem := i.(*KVItem)
		// 传递拷贝，避免外部修改
		keyCopy := append([]byte{}, kvItem.key...)
		valueCopy := append([]byte{}, kvItem.value...)
		return visitor(keyCopy, valueCopy)
	})
}

type KvItem struct {
	key   []byte
	value []byte
}
type BtreeIterator struct {
	bt        *BTreeMemTable
	KvItems   []*KvItem
	currIndex int
	length    int
}

func newBtreeIterator(bt *BTreeMemTable) *BtreeIterator {
	kvItems := make([]*KvItem, 0)
	bt.ForEach(func(key, value []byte) bool {
		kvItems = append(kvItems, &KvItem{key: key, value: value})
		return true
	})
	return &BtreeIterator{
		bt:        bt,
		KvItems:   kvItems,
		currIndex: -1, // 初始化为-1，这样第一次Next调用会将索引设为0
		length:    len(kvItems),
	}
}
func (iter *BtreeIterator) First() {
	iter.currIndex = -1 // 将索引重置为-1，这样下次Next调用会返回第一个元素
}
func (iter *BtreeIterator) Next() bool {
	iter.currIndex++
	return iter.currIndex < iter.length
}
func (iter *BtreeIterator) Key() []byte {
	return utils.CopyKey(iter.KvItems[iter.currIndex].key)
}

func (iter *BtreeIterator) Value() []byte {
	return utils.CopyKey(iter.KvItems[iter.currIndex].value)
}
