package lsm

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/memtable"
	"github.com/aixiasang/sqldb/utils"
	"github.com/aixiasang/sqldb/wal"
)

type immutableMemtable struct {
	memtable memtable.MemTable
	wal      *wal.Wal
}
type LSM struct {
	conf               *config.Config       // 配置
	mutableMemtable    memtable.MemTable    // 可变内存表
	immutableMemtables []*immutableMemtable // 不可变内存表
	currWal            *wal.Wal             // 当前WAL
	walId              uint32               // WAL ID
	levelId            []*atomic.Uint32     // 层级ID
	nodes              [][]*Node            // 节点
	sstChan            chan struct{}        // 开启压缩的通道
	compactChan        chan struct{}        // 开启压缩的通道
	isRunning          atomic.Bool          // 是否运行
	closed             atomic.Bool          // 是否关闭
	mu                 sync.RWMutex         // 互斥锁
}

// NewLSM 创建并初始化一个新的LSM树实例
func NewLSM(conf *config.Config) *LSM {
	// 设置默认值以避免nil指针
	if conf == nil {
		conf = config.NewConfig()
	}

	if conf.MaxLevel <= 0 {
		conf.MaxLevel = 7 // 默认层级数
	}

	l := &LSM{
		conf:               conf,
		immutableMemtables: make([]*immutableMemtable, 0),
		levelId:            make([]*atomic.Uint32, conf.MaxLevel),
		nodes:              make([][]*Node, conf.MaxLevel),
		compactChan:        make(chan struct{}, 100), // 增大缓冲区
		sstChan:            make(chan struct{}, 100), // 增大缓冲区
		walId:              0,                        // 初始化walId
	}

	// 初始化memtable
	l.mutableMemtable = config.NewMemTableConstructor()

	// 初始化层级ID
	for i := 0; i < conf.MaxLevel; i++ {
		l.levelId[i] = &atomic.Uint32{}
		l.nodes[i] = make([]*Node, 0)
	}
	// 创建必要的目录
	if err := l.createDirs(); err != nil {
		fmt.Printf("[ERROR] 创建目录失败: %v\n", err)
	}

	// 重置进行中的合并标记
	compactionInProgress.Store(false)

	// 标记为运行状态
	l.isRunning.Store(true)

	// 启动后台压缩线程
	go l.compactionWorker()

	// 加载SST文件
	if err := l.loadSST(); err != nil {
		fmt.Printf("[ERROR] 加载SST文件失败: %v\n", err)
	}
	// 加载WAL文件
	if err := l.loadWal(); err != nil {
		fmt.Printf("[ERROR] 加载WAL文件失败: %v\n", err)
	}

	return l
}

// createDirs 创建必要的目录
func (l *LSM) createDirs() error {
	dirs := []string{
		l.conf.DataDir,
		l.getWalDir(),
		l.getSSTDir(),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("创建目录失败: %s - %v\n", dir, err)
		}
	}

	return nil
}

// Put 写入键值对
func (l *LSM) Put(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 写入WAL
	if l.currWal != nil {
		if err := l.currWal.Write(key, value); err != nil {
			return fmt.Errorf("写入WAL失败: %v", err)
		}
	}

	// 写入内存表
	if err := l.mutableMemtable.Put(key, value); err != nil {
		return fmt.Errorf("写入内存表失败: %v", err)
	}

	// 检查是否需要切换内存表
	if l.currWal != nil && l.currWal.Size() > l.getUpperMemtableSize() {
		if err := l.switchMemtable(); err != nil {
			return fmt.Errorf("切换内存表失败: %v", err)
		}
	}

	return nil
}

// switchMemtable 切换到新的内存表
func (l *LSM) switchMemtable() error {
	fmt.Println("[DEBUG] 开始切换内存表")

	// 创建不可变内存表
	immutable := &immutableMemtable{
		memtable: l.mutableMemtable,
		wal:      l.currWal,
	}

	// 添加到不可变列表
	l.immutableMemtables = append(l.immutableMemtables, immutable)
	fmt.Printf("[DEBUG] 添加新的不可变内存表，现在共有 %d 个\n", len(l.immutableMemtables))

	// 创建新的内存表
	l.mutableMemtable = config.NewMemTableConstructor()

	// 创建新的WAL
	l.walId++
	walPath := l.getWalPath(l.walId)
	fmt.Printf("[DEBUG] 创建新的WAL文件: %s\n", walPath)

	newWal, err := wal.NewWal(l.conf, walPath)
	if err != nil {
		fmt.Printf("[ERROR] 创建新WAL失败: %v\n", err)
		// 尝试临时目录
		tempPath := fmt.Sprintf("%s/lsm_%d.wal", os.TempDir(), time.Now().UnixNano())
		fmt.Printf("[DEBUG] 尝试在临时目录创建WAL: %s\n", tempPath)

		newWal, err = wal.NewWal(l.conf, tempPath)
		if err != nil {
			fmt.Printf("[ERROR] 在临时目录创建WAL也失败: %v\n", err)
			// 继续运行，但没有WAL
			l.currWal = nil
			return nil
		}
	}

	l.currWal = newWal
	fmt.Println("[DEBUG] 新的WAL创建成功")

	// 触发压缩
	fmt.Println("[DEBUG] 发送合并信号")
	select {
	case l.compactChan <- struct{}{}:
		fmt.Println("[DEBUG] 成功发送内存表合并信号")
	default:
		fmt.Println("[WARN] 合并通道已满，使用goroutine发送")
		go func() {
			l.compactChan <- struct{}{}
			fmt.Println("[DEBUG] 通过goroutine发送合并信号成功")
		}()
	}

	return nil
}

// getUpperMemtableSize 获取内存表大小上限
func (l *LSM) getUpperMemtableSize() uint32 {
	return uint32(utils.GetCapSize(l.conf.MemTableCapSize))
}

// Get 获取键对应的值
func (l *LSM) Get(key []byte) ([]byte, bool, error) {
	if l.closed.Load() {
		return nil, false, fmt.Errorf("LSM已关闭")
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	// 1. 从可变内存表中获取
	if value, err := l.mutableMemtable.Get(key); err == nil {
		return value, true, nil
	}

	// 2. 从不可变内存表中获取
	for i := len(l.immutableMemtables) - 1; i >= 0; i-- {
		if value, err := l.immutableMemtables[i].memtable.Get(key); err == nil {
			return value, true, nil
		}
	}

	// 3. 从SSTable中获取
	for level, nodes := range l.nodes {
		// 对于0层，需要检查所有表
		if level == 0 {
			for i := len(nodes) - 1; i >= 0; i-- {
				if value, found, err := nodes[i].Get(key); err == nil && found {
					return value, true, nil
				}
			}
		} else {
			// 更高层级不会重叠
			for _, node := range nodes {
				if value, found, err := node.Get(key); err == nil && found {
					return value, true, nil
				}
			}
		}
	}

	return nil, false, nil
}

// Delete 删除键值对
func (l *LSM) Delete(key []byte) error {
	// 删除等同于写入nil值
	return l.Put(key, nil)
}

// Close 关闭LSM
func (l *LSM) Close() error {
	fmt.Println("[DEBUG] 开始关闭LSM")

	if l.closed.CompareAndSwap(false, true) {
		// 停止后台线程
		l.isRunning.Store(false)
		fmt.Println("[DEBUG] 已将 isRunning 设置为 false")

		// 等待压缩完成
		fmt.Println("[DEBUG] 等待压缩完成")
		time.Sleep(100 * time.Millisecond)

		l.mu.Lock()
		defer l.mu.Unlock()

		// 关闭WAL
		if l.currWal != nil {
			fmt.Println("[DEBUG] 关闭当前WAL")
			if err := l.currWal.Close(); err != nil {
				fmt.Printf("[ERROR] 关闭WAL失败: %v\n", err)
			}
		}

		// 关闭不可变内存表WAL
		immCount := len(l.immutableMemtables)
		fmt.Printf("[DEBUG] 关闭 %d 个不可变内存表的WAL\n", immCount)
		for i, immutable := range l.immutableMemtables {
			if immutable.wal != nil {
				if err := immutable.wal.Close(); err != nil {
					fmt.Printf("[ERROR] 关闭第 %d 个不可变内存表WAL失败: %v\n", i, err)
				}
			}
		}

		// 关闭所有节点
		nodeCount := 0
		for level, nodes := range l.nodes {
			levelCount := len(nodes)
			nodeCount += levelCount
			fmt.Printf("[DEBUG] 关闭第 %d 层的 %d 个节点\n", level, levelCount)
			for i, node := range nodes {
				if err := node.Close(); err != nil {
					fmt.Printf("[ERROR] 关闭第 %d 层第 %d 个SSTable节点失败: %v\n", level, i, err)
				}
			}
		}
		fmt.Printf("[DEBUG] 总共关闭了 %d 个SST节点\n", nodeCount)

		fmt.Println("[DEBUG] LSM关闭完成")
	} else {
		fmt.Println("[DEBUG] LSM已经关闭，无需重复操作")
	}

	return nil
}
