package lsm

import (
	"fmt"
)

func (l *LSM) getWalPath(fileId uint32) string {
	return fmt.Sprintf("%s/%s/%d.wal", l.conf.DataDir, l.conf.WalDir, fileId)
}

func (l *LSM) getWalDir() string {
	return fmt.Sprintf("%s/%s", l.conf.DataDir, l.conf.WalDir)
}
func (l *LSM) getSSTDir() string {
	return fmt.Sprintf("%s/%s", l.conf.DataDir, l.conf.SSTDir)
}
