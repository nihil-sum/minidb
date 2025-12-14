package page

import (
	"encoding/binary"
)

const (
	SizeOfPageID = 4
	SizeOfInt32  = 4
	SizeOfInt64  = 8
	SizeOfVal    = 128

	OffsetPageID     = 0
	OffsetParentID   = 4
	OffsetPageType   = 8
	OffsetCount      = 12
	OffsetNextPageID = 16
	OffsetMaxCount   = 20

	HeaderSize = 24

	// MaxDegree 28 fits safely in 4096 bytes (24 header + 28*136 = 3832)
	MaxDegree = 29
)

const (
	KindInternal = 1
	KindLeaf     = 2
)

type BPlusTreePage struct {
	Data []byte
}

func NewBPlusTreePage(p *Page) *BPlusTreePage {
	return &BPlusTreePage{Data: p.Data[:]}
}

func (p *BPlusTreePage) Init(pageID uint32, pageType uint32, parentID uint32) {
	p.SetPageID(pageID)
	p.SetPageType(pageType)
	p.SetParentID(parentID)
	p.SetCount(0)
	p.SetNextPageID(0)
}

func (p *BPlusTreePage) GetPageID() uint32 {
	return binary.LittleEndian.Uint32(p.Data[OffsetPageID : OffsetPageID+SizeOfPageID])
}
func (p *BPlusTreePage) SetPageID(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[OffsetPageID:], id)
}

func (p *BPlusTreePage) GetParentID() uint32 {
	return binary.LittleEndian.Uint32(p.Data[OffsetParentID : OffsetParentID+SizeOfPageID])
}
func (p *BPlusTreePage) SetParentID(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[OffsetParentID:], id)
}

func (p *BPlusTreePage) GetPageType() uint32 {
	return binary.LittleEndian.Uint32(p.Data[OffsetPageType : OffsetPageType+SizeOfInt32])
}
func (p *BPlusTreePage) SetPageType(kind uint32) {
	binary.LittleEndian.PutUint32(p.Data[OffsetPageType:], kind)
}

func (p *BPlusTreePage) GetCount() int32 {
	return int32(binary.LittleEndian.Uint32(p.Data[OffsetCount : OffsetCount+SizeOfInt32]))
}
func (p *BPlusTreePage) SetCount(count int32) {
	binary.LittleEndian.PutUint32(p.Data[OffsetCount:], uint32(count))
}

func (p *BPlusTreePage) GetNextPageID() uint32 {
	return binary.LittleEndian.Uint32(p.Data[OffsetNextPageID : OffsetNextPageID+SizeOfPageID])
}
func (p *BPlusTreePage) SetNextPageID(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[OffsetNextPageID:], id)
}

func (p *BPlusTreePage) IsLeaf() bool {
	return p.GetPageType() == KindLeaf
}

func (p *BPlusTreePage) getKeyOffset(index int32) int {
	slotSize := SizeOfInt64 + SizeOfVal
	if !p.IsLeaf() {
		slotSize = SizeOfInt64 + SizeOfPageID
	}
	return HeaderSize + int(index)*slotSize
}

func (p *BPlusTreePage) GetKey(index int32) int64 {
	offset := p.getKeyOffset(index)
	return int64(binary.LittleEndian.Uint64(p.Data[offset : offset+SizeOfInt64]))
}

func (p *BPlusTreePage) SetKey(index int32, key int64) {
	offset := p.getKeyOffset(index)
	binary.LittleEndian.PutUint64(p.Data[offset:], uint64(key))
}

func (p *BPlusTreePage) getPairOffset(index int32) int {
	return p.getKeyOffset(index)
}

func (p *BPlusTreePage) GetValue(index int32) []byte {
	offset := p.getPairOffset(index) + SizeOfInt64
	val := make([]byte, SizeOfVal)
	copy(val, p.Data[offset:offset+SizeOfVal])
	return val
}

func (p *BPlusTreePage) SetValue(index int32, val []byte) {
	offset := p.getPairOffset(index) + SizeOfInt64
	copy(p.Data[offset:offset+SizeOfVal], val)
}

func (p *BPlusTreePage) GetValueAsPageID(index int32) uint32 {
	offset := p.getPairOffset(index) + SizeOfInt64
	return binary.LittleEndian.Uint32(p.Data[offset : offset+SizeOfPageID])
}

func (node *BPlusTreePage) SetValueAsPageID(index int32, pageID uint32) {
	offset := node.getPairOffset(index) + SizeOfInt64
	binary.LittleEndian.PutUint32(node.Data[offset:], pageID)
}

func (node *BPlusTreePage) IsFull() bool {
	return node.GetCount() >= int32(MaxDegree-1)
}

func (node *BPlusTreePage) InsertLeaf(key int64, val []byte) bool {
	count := node.GetCount()
	index := int32(0)
	for index < count {
		currKey := node.GetKey(index)
		if currKey == key {
			return false
		}
		if currKey > key {
			break
		}
		index++
	}

	for i := count; i > index; i-- {
		node.SetKey(i, node.GetKey(i-1))
		node.SetValue(i, node.GetValue(i-1))
	}

	node.SetKey(index, key)
	node.SetValue(index, val)
	node.SetCount(count + 1)
	return true
}

func (node *BPlusTreePage) MoveHalfTo(recipient *BPlusTreePage) {
	count := node.GetCount()
	splitIdx := count / 2
	moveCount := count - splitIdx

	for i := int32(0); i < moveCount; i++ {
		srcIdx := splitIdx + i
		recipient.SetKey(i, node.GetKey(srcIdx))
		recipient.SetValue(i, node.GetValue(srcIdx))
	}

	recipient.SetCount(moveCount)
	node.SetCount(splitIdx)
}

func (p *BPlusTreePage) MinDegree() int32 {
	if p.IsLeaf() {
		return int32(MaxDegree) / 2
	}
	// 内部节点最少需要保留 MaxDegree/2 个指针
	return int32(MaxDegree+1) / 2
}

// Remove 删除指定 index 的元素
func (p *BPlusTreePage) Remove(index int32) {
	count := p.GetCount()
	if index >= count || index < 0 {
		return
	}

	// 简单的数组前移
	for i := index; i < count-1; i++ {
		p.SetKey(i, p.GetKey(i+1))
		if p.IsLeaf() {
			p.SetValue(i, p.GetValue(i+1))
		} else {
			p.SetValueAsPageID(i, p.GetValueAsPageID(i+1))
		}
	}
	p.SetCount(count - 1)
}

// MoveAllTo 将当前节点的所有元素移动到 recipient（合并）
// middleKey: 父节点中分隔这两个兄弟的 Key（仅内部节点合并时需要）
func (p *BPlusTreePage) MoveAllTo(recipient *BPlusTreePage, middleKey int64) {
	startIdx := recipient.GetCount()
	count := p.GetCount()

	// 内部节点合并时，需要先把父节点的分割 Key 拉下来放在中间
	// 这是一个简化处理，我们假设在 BPlusTree 层处理具体的 Key 逻辑，
	// 这里只负责物理搬运。

	for i := int32(0); i < count; i++ {
		recipient.SetKey(startIdx+i, p.GetKey(i))
		if p.IsLeaf() {
			recipient.SetValue(startIdx+i, p.GetValue(i))
		} else {
			recipient.SetValueAsPageID(startIdx+i, p.GetValueAsPageID(i))
		}
	}

	recipient.SetCount(startIdx + count)
	p.SetCount(0)
}

// MoveFirstToEndOf 从当前节点借第一个元素给 recipient 的末尾（Borrow From Right）
func (p *BPlusTreePage) MoveFirstToEndOf(recipient *BPlusTreePage) {
	itemKey := p.GetKey(0)

	idx := recipient.GetCount()
	recipient.SetKey(idx, itemKey)

	if p.IsLeaf() {
		recipient.SetValue(idx, p.GetValue(0))
	} else {
		recipient.SetValueAsPageID(idx, p.GetValueAsPageID(0))
	}
	recipient.SetCount(idx + 1)

	p.Remove(0)
}

// MoveLastToFrontOf 从当前节点借最后一个元素给 recipient 的头部（Borrow From Left）
func (p *BPlusTreePage) MoveLastToFrontOf(recipient *BPlusTreePage) {
	count := p.GetCount()
	itemKey := p.GetKey(count - 1)

	// Recipient 腾出位置
	recCount := recipient.GetCount()
	for i := recCount; i > 0; i-- {
		recipient.SetKey(i, recipient.GetKey(i-1))
		if recipient.IsLeaf() {
			recipient.SetValue(i, recipient.GetValue(i-1))
		} else {
			recipient.SetValueAsPageID(i, recipient.GetValueAsPageID(i-1))
		}
	}

	recipient.SetKey(0, itemKey)
	if p.IsLeaf() {
		recipient.SetValue(0, p.GetValue(count-1))
	} else {
		recipient.SetValueAsPageID(0, p.GetValueAsPageID(count-1))
	}

	recipient.SetCount(recCount + 1)
	p.SetCount(count - 1)
}
