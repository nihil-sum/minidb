package disk

import (
	"minidb/pkg/storage/page"
	"os"
	"testing"
)

func TestDiskManager(t *testing.T) {
	dbFile := "test.db"
	// 清理旧测试文件
	os.Remove(dbFile)
	defer os.Remove(dbFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}

	// 1. 分配 Page 0
	pid := dm.AllocatePage()
	if pid != 0 {
		t.Fatalf("Expected page ID 0, got %d", pid)
	}

	// 2. 创建数据并写入
	p := &page.Page{}
	data := []byte("Hello Database World!")
	copy(p.Data[:], data) // 模拟写入数据
	
	err = dm.WritePage(pid, p)
	if err != nil {
		t.Fatal(err)
	}

	// 3. 重新读取并验证
	p2 := &page.Page{}
	err = dm.ReadPage(pid, p2)
	if err != nil {
		t.Fatal(err)
	}

	readData := string(p2.Data[:len(data)])
	if readData != "Hello Database World!" {
		t.Fatalf("Data mismatch: expected %s, got %s", "Hello Database World!", readData)
	}
    
    dm.Close()
}