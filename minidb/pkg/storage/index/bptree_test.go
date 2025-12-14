package index

import (
	"minidb/pkg/buffer"
	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/page"
	"os"
	"testing"
)

func TestBPlusTreeDelete(t *testing.T) {
	file := "test_delete.db"
	_ = os.Remove(file)
	defer os.Remove(file)

	dm, _ := disk.NewDiskManager(file)
	bpm := buffer.NewBufferPoolManager(dm, 50)
	tree := NewBPlusTree(page.InvalidPageID, bpm)

	// 1. 插入数据 (0 - 100)
	n := 100
	for i := 0; i < n; i++ {
		tree.Insert(int64(i), []byte("val"))
	}

	// 2. 依次删除
	for i := 0; i < n; i++ {
		success := tree.Remove(int64(i))
		if !success {
			t.Fatalf("Failed to remove key %d", i)
		}

		// 验证确实删除了
		_, found := tree.GetValue(int64(i))
		if found {
			t.Fatalf("Key %d should not exist", i)
		}
	}

	// 3. 验证树是否为空
	if !tree.IsEmpty() {
		t.Fatal("Tree should be empty after removing all keys")
	}
}
