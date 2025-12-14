package buffer

import (
	"errors"
	"sync"

	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/page"
)

type BufferPoolManager struct {
	mu          sync.Mutex
	diskManager disk.DiskManager
	pages       []*page.Page        // 实际的内存池 (数组大小固定)
	replacer    *LRUReplacer        // LRU 替换算法
	freeList    []int               // 空闲的 FrameID 列表
	pageTable   map[page.PageID]int // 映射表: PageID -> FrameID
}

// NewBufferPoolManager 初始化
func NewBufferPoolManager(diskManager disk.DiskManager, poolSize int) *BufferPoolManager {
	bpm := &BufferPoolManager{
		diskManager: diskManager,
		pages:       make([]*page.Page, poolSize),
		replacer:    NewLRUReplacer(poolSize),
		freeList:    make([]int, poolSize),
		pageTable:   make(map[page.PageID]int),
	}

	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = &page.Page{} // 预分配内存对象
		bpm.freeList[i] = i         // 初始时所有 Frame 都是空闲的
	}

	return bpm
}

// FetchPage 核心方法：获取一个页面
// 1. 如果在缓存中，直接返回
// 2. 如果不在，从磁盘读取到缓存（可能需要驱逐旧页）
func (b *BufferPoolManager) FetchPage(pageID page.PageID) *page.Page {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. 缓存命中 (Cache Hit)
	if frameID, ok := b.pageTable[pageID]; ok {
		b.replacer.Pin(frameID) // 标记为正在使用，阻止被 LRU 驱逐
		p := b.pages[frameID]
		p.SetPinCount(p.PinCount() + 1)
		return p
	}

	// 2. 缓存未命中 (Cache Miss)，需要找一个空闲 Frame
	frameID, err := b.findVictimFrame()
	if err != nil {
		return nil // 内存满了且所有页都被钉住(Pinned)，无法读取新页
	}

	// 3. 从磁盘读取数据
	p := b.pages[frameID]
	// 注意：findVictimFrame 已经处理了脏页刷盘和旧映射移除

	// 重置 Page 对象元数据
	p.SetID(pageID)
	p.SetPinCount(1)
	p.SetDirty(false)

	// 真正读取
	err = b.diskManager.ReadPage(pageID, p)
	if err != nil {
		return nil
	}

	// 4. 更新映射表和 LRU
	b.pageTable[pageID] = frameID
	b.replacer.Pin(frameID)

	return p
}

// UnpinPage 核心方法：释放一个页面
// isDirty: 如果调用者修改了页面，必须传 true
func (b *BufferPoolManager) UnpinPage(pageID page.PageID, isDirty bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	frameID, ok := b.pageTable[pageID]
	if !ok {
		return errors.New("try to unpin a page not in buffer pool")
	}

	p := b.pages[frameID]
	if p.PinCount() <= 0 {
		return errors.New("pin count is already 0")
	}

	// 递减引用计数
	p.SetPinCount(p.PinCount() - 1)

	// 如果是脏的，标记一下（注意是 OR 操作，不能把脏页标记回干净）
	if isDirty {
		p.SetDirty(true)
	}

	// 如果没人用了，通知 LRU 算法这个 Frame 可以被淘汰了
	if p.PinCount() == 0 {
		b.replacer.Unpin(frameID)
	}

	return nil
}

// NewPage 分配一个新的磁盘页，并将其放入缓存
func (b *BufferPoolManager) NewPage() *page.Page {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. 寻找空闲 Frame
	frameID, err := b.findVictimFrame()
	if err != nil {
		return nil
	}

	// 2. 在磁盘分配新 PageID
	newPageID := b.diskManager.AllocatePage()

	// 3. 初始化内存页对象
	p := b.pages[frameID]
	p.SetID(newPageID)
	p.SetPinCount(1)
	p.SetDirty(false) // 新页一开始是空的，不算脏（或者看作全是0）
	p.Clear()         // 清空之前遗留的数据

	// 4. 更新映射
	b.pageTable[newPageID] = frameID
	b.replacer.Pin(frameID)

	return p
}

// FlushPage 强制将某个页面刷盘
func (b *BufferPoolManager) FlushPage(pageID page.PageID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	frameID, ok := b.pageTable[pageID]
	if !ok {
		return false
	}

	p := b.pages[frameID]
	b.diskManager.WritePage(pageID, p)
	p.SetDirty(false) // 刷盘后变干净了
	return true
}

// findVictimFrame 辅助方法：寻找可用的 FrameID
// 如果 freeList 有空闲，直接用；否则从 LRU 驱逐一个
func (b *BufferPoolManager) findVictimFrame() (int, error) {
	// 1. 优先从 FreeList 拿
	if len(b.freeList) > 0 {
		frameID := b.freeList[0]
		b.freeList = b.freeList[1:]
		return frameID, nil
	}

	// 2. FreeList 空了，求助 LRU 算法
	frameID := b.replacer.Victim()
	if frameID == -1 {
		return -1, errors.New("no victim found (all pages are pinned)")
	}

	// 3. 驱逐旧页前，检查是否需要写回磁盘 (Eviction Logic)
	victimPage := b.pages[frameID]
	if victimPage.IsDirty() {
		b.diskManager.WritePage(victimPage.ID(), victimPage)
	}

	// 4. 从映射表中移除旧页 ID
	delete(b.pageTable, victimPage.ID())

	return frameID, nil
}
func (b *BufferPoolManager) DeletePage(pageID page.PageID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	frameID, ok := b.pageTable[pageID]
	if !ok {
		// 页面不在内存中，直接通知磁盘释放
		b.diskManager.DeallocatePage(pageID)
		return true
	}

	targetPage := b.pages[frameID]

	// 如果页面被钉住（正在使用），则无法删除
	if targetPage.PinCount() > 0 {
		return false
	}

	// 1. 从页表中移除
	delete(b.pageTable, pageID)

	// 2. 停止 LRU 追踪（因为它已经不存在了）
	b.replacer.Pin(frameID) // Pin 会将其从 LRU 列表中移除
	// 注意：这里我们不需要再 Unpin，因为我们要手动将其放入 FreeList

	// 3. 将 Frame 放回空闲列表
	b.freeList = append(b.freeList, frameID)

	// 4. 重置内存页元数据
	targetPage.SetID(page.InvalidPageID)
	targetPage.SetPinCount(0)
	targetPage.SetDirty(false)

	// 5. 通知磁盘释放
	b.diskManager.DeallocatePage(pageID)

	return true
}
func (b *BufferPoolManager) FlushAllPages() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range b.pages {
		// page.InvalidPageID 通常定义为 -1，确保 page 包已导出该常量
		// 如果 p.ID() 是有效的且是脏页，则刷盘
		if p.ID() != page.InvalidPageID && p.IsDirty() {
			b.diskManager.WritePage(p.ID(), p)
			p.SetDirty(false)
		}
	}
}
