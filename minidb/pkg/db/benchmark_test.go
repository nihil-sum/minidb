package db

import (
	"fmt"
	"minidb/pkg/buffer"
	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/index"
	"minidb/pkg/storage/page"
	"os"
	"testing"
	"time"
)

// 这是一个专门的性能测试函数
// 运行命令: go test -v minidb/pkg/db -run TestBenchmark
func TestBenchmark(t *testing.T) {
	// 1. 配置测试环境
	dbFile := "bench.db"
	metaFile := "bench.meta"
	os.Remove(dbFile)
	os.Remove(metaFile)
	defer os.Remove(dbFile)
	defer os.Remove(metaFile)

	// 初始化引擎组件 (直接绕过 Server 网络层，测试纯内核性能)
	dm, _ := disk.NewDiskManager(dbFile)
	bpm := buffer.NewBufferPoolManager(dm, 1000) // 给大一点缓存 1000页
	tree := index.NewBPlusTree(page.InvalidPageID, bpm)
	tree.StartNewTree() // 创建根节点

	// 2. 准备数据量
	const DataCount = 10000 // 插入 1万条数据

	fmt.Println("🚀 Starting Benchmark...")
	fmt.Printf("TARGET: Insert %d keys, then Select %d keys.\n", DataCount, DataCount)
	fmt.Println("------------------------------------------------")

	// --- 阶段一：写入性能 (Insert) ---
	startInsert := time.Now()

	for i := 0; i < DataCount; i++ {
		key := int64(i)
		// 模拟 100 字节的数据
		val := fmt.Sprintf("data-%090d", i)
		tree.Insert(key, []byte(val))
	}

	// 强制刷盘，确保数据落地的开销也算在内 (可选，看你想测纯内存还是落盘)
	bpm.FlushAllPages()

	durationInsert := time.Since(startInsert)
	opsInsert := float64(DataCount) / durationInsert.Seconds()

	fmt.Printf("✅ Insert Done.\n")
	fmt.Printf("   Time: %v\n", durationInsert)
	fmt.Printf("   TPS:  %.2f ops/sec\n", opsInsert)
	fmt.Println("------------------------------------------------")

	// --- 阶段二：读取性能 (Select) ---
	startSelect := time.Now()

	for i := 0; i < DataCount; i++ {
		key := int64(i)
		val, found := tree.GetValue(key)
		if !found || len(val) == 0 {
			t.Errorf("Key %d lost!", i)
		}
	}

	durationSelect := time.Since(startSelect)
	opsSelect := float64(DataCount) / durationSelect.Seconds()

	fmt.Printf("✅ Select Done.\n")
	fmt.Printf("   Time: %v\n", durationSelect)
	fmt.Printf("   QPS:  %.2f ops/sec\n", opsSelect)
	fmt.Println("------------------------------------------------")
}
