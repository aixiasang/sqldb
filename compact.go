package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/sstable"
)

// 默认L0阈值，应从config读取
const defaultLevel0CompactThreshold = 4

// 标记当前正在进行合并的状态
var compactionInProgress atomic.Bool

func (l *LSM) compactionWorker() {
	fmt.Println("[DEBUG] 合并工作线程启动")
	defer fmt.Println("[DEBUG] 合并工作线程退出")

	// 启动另一个worker以增加处理能力
	go func() {
		fmt.Println("[DEBUG] 辅助合并工作线程启动")
		for {
			if !l.isRunning.Load() {
				fmt.Println("[DEBUG] LSM已经停止运行，辅助合并工作线程退出")
				return
			}

			// 检查是否有需要合并的内存表
			l.mu.RLock()
			immCount := len(l.immutableMemtables)
			l.mu.RUnlock()

			if immCount > 0 && !compactionInProgress.Load() {
				if compactionInProgress.CompareAndSwap(false, true) {
					fmt.Printf("[DEBUG] 辅助线程检测到有 %d 个未合并的不可变内存表，触发合并\n", immCount)
					l.compactMemTables()
					compactionInProgress.Store(false)
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	for {
		select {
		case _, ok := <-l.compactChan:
			if !ok {
				fmt.Println("[DEBUG] compactChan已关闭，合并工作线程退出")
				return
			}
			if !l.isRunning.Load() {
				fmt.Println("[DEBUG] LSM已经停止运行，合并工作线程退出")
				return
			}

			// 避免并发合并
			if compactionInProgress.CompareAndSwap(false, true) {
				fmt.Println("[DEBUG] 收到内存表合并信号，开始合并...")
				l.compactMemTables()
				compactionInProgress.Store(false)
			} else {
				fmt.Println("[DEBUG] 正在进行合并，忽略本次合并请求")
			}

		case _, ok := <-l.sstChan:
			if !ok {
				fmt.Println("[DEBUG] sstChan已关闭，合并工作线程退出")
				return
			}
			if !l.isRunning.Load() {
				fmt.Println("[DEBUG] LSM已经停止运行，合并工作线程退出")
				return
			}
			fmt.Println("[DEBUG] 收到SST合并信号，开始合并...")
			l.compactSSTables()

		case <-time.After(1 * time.Second): // 添加超时检查，确保线程没有永久阻塞
			if !l.isRunning.Load() {
				fmt.Println("[DEBUG] LSM已经停止运行，合并工作线程退出")
				return
			}

			// 主动检查是否有需要合并的内存表
			l.mu.RLock()
			immCount := len(l.immutableMemtables)
			l.mu.RUnlock()

			if immCount > 0 && !compactionInProgress.Load() {
				if compactionInProgress.CompareAndSwap(false, true) {
					fmt.Printf("[DEBUG] 定时检测到有 %d 个未合并的不可变内存表，触发合并\n", immCount)
					l.compactMemTables()
					compactionInProgress.Store(false)
				}
			}
		}
	}
}

// compactMemTables 将不可变memtable转换为SST文件
func (l *LSM) compactMemTables() {
	fmt.Println("[DEBUG] 开始执行 compactMemTables")
	startTime := time.Now()

	l.mu.Lock()
	if len(l.immutableMemtables) == 0 {
		fmt.Println("[DEBUG] 没有不可变内存表需要合并，退出")
		l.mu.Unlock()
		return
	}

	// 取出第一个不可变memtable
	immutable := l.immutableMemtables[0]
	l.immutableMemtables = l.immutableMemtables[1:]
	immCount := len(l.immutableMemtables)
	fmt.Printf("[DEBUG] 取出一个不可变内存表，剩余 %d 个\n", immCount)
	l.mu.Unlock()

	// 检查memtable是否为空
	if immutable == nil || immutable.memtable == nil {
		fmt.Println("[ERROR] 不可变内存表或内存表为空")
		return
	}

	// 检查memtable中的数据
	iter := immutable.memtable.Iterator()
	fmt.Println("[DEBUG] 开始遍历内存表数据")
	dataCount := 0
	for iter.Next() {
		dataCount++
	}
	fmt.Printf("[DEBUG] 内存表中有 %d 条数据\n", dataCount)
	if dataCount == 0 {
		fmt.Println("[WARN] 内存表中没有数据，跳过SST创建")
		// 仍需关闭并删除WAL
		if immutable.wal != nil {
			walPath := immutable.wal.FilePath()
			if err := immutable.wal.Delete(); err != nil {
				fmt.Printf("[ERROR] 删除空内存表的WAL文件失败: %v\n", err)
			} else {
				fmt.Printf("[DEBUG] 成功删除空内存表的WAL文件: %s\n", filepath.Base(walPath))
			}
		}
		return
	}

	// 将memtable转换为SST文件
	seq := l.levelId[0].Add(1) - 1
	sstPath := fmt.Sprintf("%s/0_%d.sst", l.getSSTDir(), seq)

	fmt.Printf("[DEBUG] 开始将内存表转换为SST文件: %s\n", sstPath)

	// 确保目录存在
	sstDir := filepath.Dir(sstPath)
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		fmt.Printf("[ERROR] 创建SST目录失败: %v\n", err)
		return
	}
	fmt.Printf("[DEBUG] 确认目录存在: %s\n", sstDir)

	// 创建SST Writer
	writer, err := sstable.NewSSTWriter(sstPath, l.conf)
	if err != nil {
		fmt.Printf("[ERROR] 创建SST Writer失败: %v\n", err)
		return
	}
	fmt.Println("[DEBUG] 成功创建SST Writer")

	// 将memtable数据写入SST
	fmt.Println("[DEBUG] 开始将内存表数据写入SST")
	if err := writer.Write(immutable.memtable); err != nil {
		fmt.Printf("[ERROR] 写入SST文件失败: %v\n", err)
		writer.Close()
		// 如果写入失败，删除可能已部分创建的文件
		os.Remove(sstPath)
		return
	}
	fmt.Println("[DEBUG] 成功将内存表数据写入SST")

	// 关闭SST写入器
	if err := writer.Close(); err != nil {
		fmt.Printf("[ERROR] 关闭SST文件失败: %v\n", err)
		// 如果关闭失败，删除可能已部分创建的文件
		os.Remove(sstPath)
		return
	}
	fmt.Println("[DEBUG] 成功关闭SST Writer")

	fmt.Printf("[INFO] 成功创建SST文件: %s\n", sstPath)

	// 添加到0级
	node, err := NewNode(l.conf, 0, seq)
	if err != nil {
		fmt.Printf("[ERROR] 创建SST节点失败: %v\n", err)
		os.Remove(sstPath)
		return
	}
	fmt.Println("[DEBUG] 成功创建SST节点")

	l.mu.Lock()
	l.nodes[0] = append(l.nodes[0], node)
	level0Count := len(l.nodes[0])
	fmt.Printf("[DEBUG] 成功将SST节点添加到0级，现在0级有 %d 个节点\n", level0Count)
	l.mu.Unlock()

	// 获取WAL文件路径并关闭WAL
	if immutable.wal != nil {
		// 使用 WAL 的 Delete 方法一步完成关闭和删除
		walPath := immutable.wal.FilePath()
		fmt.Printf("[DEBUG] 准备删除WAL文件: %s\n", walPath)
		if err := immutable.wal.Delete(); err != nil {
			fmt.Printf("[ERROR] 删除WAL文件失败: %v\n", err)
		} else {
			fmt.Printf("[INFO] 成功删除WAL文件: %s\n", filepath.Base(walPath))
		}
	} else {
		fmt.Println("[WARN] 没有WAL文件，跳过删除")
	}

	// 检查是否需要继续压缩
	l.mu.RLock()
	immCount = len(l.immutableMemtables)
	l.mu.RUnlock()

	if immCount > 0 {
		fmt.Printf("[DEBUG] 还有 %d 个不可变内存表需要合并，继续合并操作\n", immCount)
		// 不需要发送信号，直接在当前线程继续处理
		l.compactMemTables()
	} else {
		fmt.Println("[DEBUG] 没有更多不可变内存表需要合并")
	}

	// 检查L0是否需要压缩到L1
	l.mu.RLock()
	level0Count = len(l.nodes[0])
	l.mu.RUnlock()

	if level0Count > defaultLevel0CompactThreshold {
		fmt.Printf("[DEBUG] L0级节点数量(%d)超过阈值(%d)，需要进行SST合并\n",
			level0Count, defaultLevel0CompactThreshold)
		select {
		case l.sstChan <- struct{}{}:
			fmt.Println("[DEBUG] 成功发送SST合并信号")
		default:
			fmt.Println("[WARN] SST合并通道已满，跳过发送信号")
		}
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("[DEBUG] compactMemTables 执行完成，耗时: %v\n", elapsedTime)
}

// compactSSTables 合并SST文件到下一层
func (l *LSM) compactSSTables() {
	fmt.Println("[DEBUG] 开始执行 compactSSTables")

	// 简单实现，仅检查L0是否超过阈值
	l.mu.Lock()
	defer l.mu.Unlock()

	level0Count := len(l.nodes[0])
	if level0Count <= defaultLevel0CompactThreshold {
		fmt.Printf("[DEBUG] L0级节点数量(%d)未超过阈值(%d)，无需合并\n",
			level0Count, defaultLevel0CompactThreshold)
		return
	}

	// 这里应该实现复杂的合并逻辑，但本示例仅空实现
	fmt.Printf("[INFO] 进行SST合并操作，当前L0有%d个文件\n", level0Count)

	// TODO: 实现SST文件的合并逻辑

	fmt.Println("[DEBUG] compactSSTables 执行完成")
}

// createSSTFromMemtable 从memtable创建SST文件
func createSSTFromMemtable(memtable interface{}, conf *config.Config, path string) (interface{}, error) {
	// 根据实际实现来创建SST文件
	// 这里是一个简化的空实现
	return nil, nil
}
