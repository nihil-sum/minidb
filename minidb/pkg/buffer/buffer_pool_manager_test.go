package buffer

import (
	"os"
	"testing"

	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestBufferPoolManager(t *testing.T) {
	dbFile := "test_bpm.db"
	os.Remove(dbFile)
	defer os.Remove(dbFile)

	dm, _ := disk.NewDiskManager(dbFile)
	// 创建一个只有 2 个 Frame 的缓冲池
	bpm := NewBufferPoolManager(dm, 2)

	// 1. 创建 Page 0
	p0 := bpm.NewPage()
	assert.NotNil(t, p0)
	assert.Equal(t, page.PageID(0), p0.ID())

	// 写入一些数据到 Page 0，并标记为脏
	copy(p0.Data[:], []byte("Page 0 Data"))
	bpm.UnpinPage(0, true) // Unpin, dirty=true

	// 2. 创建 Page 1
	p1 := bpm.NewPage()
	assert.NotNil(t, p1)
	assert.Equal(t, page.PageID(1), p1.ID())
	copy(p1.Data[:], []byte("Page 1 Data"))
	bpm.UnpinPage(1, true)

	// 此时 Pool 满了: [Page0(LRU), Page1(MRU)]

	// 3. 创建 Page 2 -> 应该触发 Page 0 被驱逐 (Evict) 并刷盘
	p2 := bpm.NewPage()
	assert.NotNil(t, p2)
	assert.Equal(t, page.PageID(2), p2.ID())
	copy(p2.Data[:], []byte("Page 2 Data"))
	bpm.UnpinPage(2, false)

	// 4. 再次读取 Page 0 -> 应该从磁盘读回来 (包含之前的写入)
	p0_read := bpm.FetchPage(0)
	assert.NotNil(t, p0_read)
	// 验证数据是否还在 (说明驱逐时正确刷盘了)
	assert.Equal(t, "Page 0 Data", string(p0_read.Data[:11]))
	
	// 此时 Pool: [Page1(被驱逐), Page2, Page0] -> Page 1 应该不在内存了
	
	// 5. 验证 Page 1 是否真的被驱逐了 (Fetch 应该不会影响 PinCount，因为是重新加载的)
	// 这里的验证比较隐晦，主要看是否触发了 Disk Read
	p1_read := bpm.FetchPage(1)
	assert.NotNil(t, p1_read)
	assert.Equal(t, "Page 1 Data", string(p1_read.Data[:11]))

	bpm.UnpinPage(0, false)
	bpm.UnpinPage(1, false)
}