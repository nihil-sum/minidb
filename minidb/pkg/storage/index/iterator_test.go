package index

import (
	"encoding/binary"
	"math/rand"
	"minidb/pkg/buffer"
	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/page" // 确保使用了它，或者如果真没用就删掉
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTreeIterator(t *testing.T) {
	file := "test_iterator.db"
	_ = os.Remove(file)
	defer os.Remove(file)

	// 修复 1: 处理 NewDiskManager 的 error 返回值
	diskManager, err := disk.NewDiskManager(file)
	assert.Nil(t, err)

	bpm := buffer.NewBufferPoolManager(diskManager, 100)

	// 修复 2: NewBPlusTree 需要传入初始 RootPageID。
	// 因为是新树，我们传 InvalidPageID 让它自己去初始化
	tree := NewBPlusTree(page.InvalidPageID, bpm)

	n := 2000
	rand.Seed(time.Now().UnixNano())
	keys := rand.Perm(n)

	t.Logf("Inserting %d keys...", n)
	for _, k := range keys {
		key := int64(k)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(k*10))
		tree.Insert(key, val)
	}

	t.Log("Starting Iterator Scan...")

	it := tree.Begin()
	assert.NotNil(t, it, "Iterator should not be nil")
	defer it.Close()

	var expectedKey int64 = 0
	count := 0

	assert.Equal(t, expectedKey, it.Key())

	for {
		if it.Key() != expectedKey {
			t.Errorf("Order Broken! Expected %d, but got %d", expectedKey, it.Key())
			// 遇到错误可以提前退出防止刷屏
			break
		}

		val := it.Value()
		valInt := int64(binary.BigEndian.Uint64(val))
		if valInt != expectedKey*10 {
			t.Errorf("Value Broken! Expected %d, but got %d", expectedKey*10, valInt)
		}

		expectedKey++
		count++

		if !it.Next() {
			break
		}
	}

	assert.Equal(t, n, count, "Iterator did not visit all records")
	t.Logf("Successfully iterated over %d records.", count)
}
