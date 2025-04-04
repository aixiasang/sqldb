package lsm

import (
	"os"
	"sort"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/utils"
	"github.com/aixiasang/sqldb/wal"
)

func (l *LSM) loadWal() error {
	walDir := l.getWalDir()
	files, err := os.ReadDir(walDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		curWal, err := wal.NewWal(l.conf, l.getWalPath(l.walId))
		if err != nil {
			return err
		}
		l.mutableMemtable = config.NewMemTableConstructor()
		l.currWal = curWal
		return nil
	}
	walFileIds := make([]uint32, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileId, err := utils.ParseWalPath(file.Name())
		if err != nil {
			return err
		}
		walFileIds = append(walFileIds, fileId)
	}
	sort.Slice(walFileIds, func(i, j int) bool {
		return walFileIds[i] < walFileIds[j]
	})
	for i, fileId := range walFileIds {
		wal, err := wal.NewWal(l.conf, l.getWalPath(fileId))
		if err != nil {
			return err
		}
		curMemtable := config.NewMemTableConstructor()
		if err := wal.ReadAll(curMemtable); err != nil {
			return err
		}
		if i == len(walFileIds)-1 {
			l.currWal = wal
			l.mutableMemtable = curMemtable
		} else {
			l.immutableMemtables = append(l.immutableMemtables, &immutableMemtable{
				memtable: curMemtable,
				wal:      wal,
			})
			//发送合并信息号
			go func() {
				l.compactChan <- struct{}{}
			}()
		}
	}
	return nil
}

type tempSST struct {
	level int
	seq   uint32
	path  string
}

func (l *LSM) loadSST() error {
	sstDir := l.getSSTDir()
	files, err := os.ReadDir(sstDir)
	if err != nil {
		return err
	}

	sstFiles := make([]*tempSST, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		level, seq, err := utils.ParseSSTPath(file.Name())
		if err != nil {
			return err
		}
		sstFiles = append(sstFiles, &tempSST{
			level: level,
			seq:   seq,
			path:  file.Name(),
		})
	}
	sort.Slice(sstFiles, func(i, j int) bool {
		if sstFiles[i].level != sstFiles[j].level {
			return sstFiles[i].level < sstFiles[j].level
		}
		return sstFiles[i].seq < sstFiles[j].seq
	})
	for _, sstFile := range sstFiles {
		node, err := NewNode(l.conf, sstFile.level, sstFile.seq)
		if err != nil {
			return err
		}
		l.nodes[sstFile.level] = append(l.nodes[sstFile.level], node)
	}
	return nil
}
