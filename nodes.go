package lsm

import (
	"fmt"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/sstable"
)

type Node struct {
	conf   *config.Config
	reader *sstable.SSTReader
	level  int
	seq    uint32
}

func NewNode(conf *config.Config, level int, seq uint32) (*Node, error) {
	node := &Node{
		conf:  conf,
		level: level,
		seq:   seq,
	}
	filePath := fmt.Sprintf("%s/%s/%d_%d.sst", node.conf.DataDir, node.conf.SSTDir, level, seq)
	reader, err := sstable.NewSSTReader(filePath, node.conf)
	if err != nil {
		return nil, err
	}
	node.reader = reader
	return node, nil
}
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	return n.reader.Get(key)
}
func (n *Node) Close() error {
	return n.reader.Close()
}

// Iterator 返回此SSTable节点的迭代器
func (n *Node) Iterator() sstable.Iterator {
	return n.reader.Iterator()
}
