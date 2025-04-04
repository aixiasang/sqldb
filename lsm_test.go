package lsm

import (
	"fmt"
	"testing"
	"time"

	"github.com/aixiasang/sqldb/config"
	"github.com/aixiasang/sqldb/utils"
)

func TestLsmPut(t *testing.T) {
	conf := config.NewConfig()
	// 使用更小的内存表容量以更快触发合并
	conf.MemTableCapSize = 64
	fmt.Println("[TEST] 创建LSM实例，内存表容量:", conf.MemTableCapSize)

	lsm := NewLSM(conf)
	if lsm == nil {
		t.Fatal("无法创建LSM实例")
	}

	// 确保测试结束后资源被正确清理
	defer func() {
		fmt.Println("[TEST] 等待3秒，确保合并操作完成...")
		time.Sleep(3 * time.Second)
		fmt.Println("[TEST] 开始关闭LSM")
		if err := lsm.Close(); err != nil {
			t.Logf("关闭LSM时出错: %v", err)
		}
		fmt.Println("[TEST] LSM已关闭")
	}()

	// 添加200个键值对，确保触发多次内存表切换和压缩
	dataCount := 200
	fmt.Printf("[TEST] 开始写入 %d 个键值对\n", dataCount)
	d := make(map[string]string)
	for i := 0; i < dataCount; i++ {
		key, value := utils.GenerateKey(i), utils.GenerateValue(i)
		d[string(key)] = string(value)
		if err := lsm.Put(key, value); err != nil {
			t.Fatalf("写入失败 [%d]: %v", i, err)
		}

		// 每写入50个键值对打印一次状态
		if (i+1)%50 == 0 {
			fmt.Printf("[TEST] 已写入 %d/%d 个键值对\n", i+1, dataCount)
		}
	}
	fmt.Printf("[TEST] 全部 %d 个键值对写入完成\n", dataCount)

	// 等待1秒，确保有足够时间进行合并
	fmt.Println("[TEST] 等待1秒，让合并操作进行...")
	time.Sleep(1 * time.Second)

	// 验证写入的数据
	fmt.Println("[TEST] 开始验证数据正确性")
	verifyCount := 0
	failCount := 0
	for key, expectedValue := range d {
		value, found, err := lsm.Get([]byte(key))
		if err != nil {
			t.Errorf("获取键 %s 失败: %v", key, err)
			failCount++
			continue
		}
		if !found {
			t.Errorf("键 %s 未找到", key)
			failCount++
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("键 %s 的值不匹配: 期望=%s, 实际=%s", key, expectedValue, string(value))
			failCount++
			continue
		}
		verifyCount++

		// 每验证50个键值对打印一次状态
		if verifyCount%50 == 0 {
			fmt.Printf("[TEST] 已验证 %d/%d 个键值对\n", verifyCount, dataCount)
		}
	}

	fmt.Printf("[TEST] 数据验证完成: 成功=%d, 失败=%d\n", verifyCount, failCount)

	// 打印LSM状态
	lsm.mu.RLock()
	fmt.Printf("[TEST] LSM状态: 不可变内存表=%d, L0节点=%d\n",
		len(lsm.immutableMemtables), len(lsm.nodes[0]))
	lsm.mu.RUnlock()

	if failCount == 0 {
		t.Logf("成功测试完成 %d 个键值对的写入和读取", dataCount)
	} else {
		t.Errorf("测试失败: %d 个键值对验证失败", failCount)
	}
}

// 专门测试合并功能
func TestLsmCompaction(t *testing.T) {
	conf := config.NewConfig()
	// 使用更小的内存表容量和L0合并阈值以快速触发合并
	conf.MemTableCapSize = 32
	fmt.Println("[TEST] 创建LSM实例，内存表容量:", conf.MemTableCapSize)

	lsm := NewLSM(conf)
	if lsm == nil {
		t.Fatal("无法创建LSM实例")
	}

	// 确保测试结束后资源被正确清理
	defer func() {
		fmt.Println("[TEST] 等待5秒，确保合并操作完成...")
		time.Sleep(5 * time.Second)
		fmt.Println("[TEST] 开始关闭LSM")
		if err := lsm.Close(); err != nil {
			t.Logf("关闭LSM时出错: %v", err)
		}
		fmt.Println("[TEST] LSM已关闭")
	}()

	// 添加多批次数据，每批50个，共300个
	batchCount := 6
	batchSize := 50
	totalCount := batchCount * batchSize

	fmt.Printf("[TEST] 开始分批写入共 %d 个键值对\n", totalCount)
	for b := 0; b < batchCount; b++ {
		fmt.Printf("[TEST] 开始写入第 %d/%d 批数据\n", b+1, batchCount)

		// 写入一批数据
		for i := 0; i < batchSize; i++ {
			key := []byte(fmt.Sprintf("batch%d-key%d", b, i))
			value := []byte(fmt.Sprintf("batch%d-value%d", b, i))
			if err := lsm.Put(key, value); err != nil {
				t.Fatalf("写入失败 [批次=%d, 索引=%d]: %v", b, i, err)
			}
		}

		fmt.Printf("[TEST] 第 %d/%d 批数据写入完成\n", b+1, batchCount)

		// 每写入一批后打印LSM状态并等待1秒观察合并
		lsm.mu.RLock()
		immCount := len(lsm.immutableMemtables)
		l0Count := len(lsm.nodes[0])
		lsm.mu.RUnlock()

		fmt.Printf("[TEST] 当前LSM状态: 不可变内存表=%d, L0节点=%d\n", immCount, l0Count)

		// 小暂停让合并有机会发生
		fmt.Println("[TEST] 等待2秒，观察合并操作...")
		time.Sleep(2 * time.Second)
	}

	// 再次等待确保所有合并完成
	fmt.Println("[TEST] 所有数据写入完成，再等待3秒确保合并完成...")
	time.Sleep(3 * time.Second)

	// 读取所有数据验证正确性
	fmt.Println("[TEST] 开始验证所有数据")
	errorCount := 0
	for b := 0; b < batchCount; b++ {
		for i := 0; i < batchSize; i++ {
			key := []byte(fmt.Sprintf("batch%d-key%d", b, i))
			expectedValue := []byte(fmt.Sprintf("batch%d-value%d", b, i))

			value, found, err := lsm.Get(key)
			if err != nil {
				t.Errorf("获取键失败 [批次=%d, 索引=%d]: %v", b, i, err)
				errorCount++
				continue
			}
			if !found {
				t.Errorf("键未找到 [批次=%d, 索引=%d]", b, i)
				errorCount++
				continue
			}
			if string(value) != string(expectedValue) {
				t.Errorf("值不匹配 [批次=%d, 索引=%d]: 期望=%s, 实际=%s",
					b, i, string(expectedValue), string(value))
				errorCount++
				continue
			}
		}

		fmt.Printf("[TEST] 已验证第 %d/%d 批数据\n", b+1, batchCount)
	}

	// 最终状态
	lsm.mu.RLock()
	immCount := len(lsm.immutableMemtables)
	l0Count := len(lsm.nodes[0])
	lsm.mu.RUnlock()

	fmt.Printf("[TEST] 测试完成，最终LSM状态: 不可变内存表=%d, L0节点=%d, 验证错误=%d\n",
		immCount, l0Count, errorCount)

	if errorCount == 0 {
		t.Logf("合并测试成功，所有 %d 个键值对均正确验证", totalCount)
	} else {
		t.Errorf("合并测试失败，有 %d 个键值对验证出错", errorCount)
	}
}
