package buffer

import (
	"container/list"
	"sync"
)

// LRUReplacer 负责追踪页面使用情况，决定驱逐哪个页面
// 这里管理的不是 PageID，而是 FrameID (缓冲池数组的索引)
type LRUReplacer struct {
	mu       sync.Mutex
	capacity int
	list     *list.List              // 双向链表，头部是最近使用，尾部是最久未用
	elements map[int]*list.Element   // 快速查找 FrameID 对应的链表节点
}

func NewLRUReplacer(capacity int) *LRUReplacer {
	return &LRUReplacer{
		capacity: capacity,
		list:     list.New(),
		elements: make(map[int]*list.Element),
	}
}

// Victim 移除并返回最久未使用的 FrameID (链表尾部)
// 如果没有可移除的元素，返回 -1
func (l *LRUReplacer) Victim() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.list.Len() == 0 {
		return -1
	}

	// 取出链表尾部 (最久未使用)
	elem := l.list.Back()
	frameID := elem.Value.(int)

	// 移除
	l.list.Remove(elem)
	delete(l.elements, frameID)

	return frameID
}

// Pin 当一个页面被正在使用时，它不应该被 LRU 驱逐
// 所以 Pin 操作实际上是从 LRU 列表中**移除**该 FrameID
// 等到 Unpin 时再加回来
func (l *LRUReplacer) Pin(frameID int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.elements[frameID]; ok {
		l.list.Remove(elem)
		delete(l.elements, frameID)
	}
}

// Unpin 当页面不再被使用时，调用 Unpin
// 将其加入 LRU 列表（视为最近使用，放在头部）
func (l *LRUReplacer) Unpin(frameID int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.elements[frameID]; ok {
		return // 已经在列表中了
	}

	if l.list.Len() >= l.capacity {
		// 如果满了，一般不需要处理，因为 Manager 会先调用 Victim 腾位置
		// 但为了健壮性，这里可以忽略或者把尾部的挤掉
		return 
	}

	elem := l.list.PushFront(frameID)
	l.elements[frameID] = elem
}

func (l *LRUReplacer) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.list.Len()
}