package index

import (
	"minidb/pkg/buffer"
	"minidb/pkg/storage/page"
)

// TreeIterator 是 B+ 树的迭代器，用于遍历叶子节点
type TreeIterator struct {
	bpm      *buffer.BufferPoolManager
	currPage *page.BPlusTreePage // 当前被 Pin 住的页
	currIdx  int32               // 当前页内的 Slot Index
}

// NewTreeIterator 创建一个新的迭代器 (通常由 BPlusTree 调用)
func NewTreeIterator(bpm *buffer.BufferPoolManager, page *page.BPlusTreePage, idx int32) *TreeIterator {
	return &TreeIterator{
		bpm:      bpm,
		currPage: page,
		currIdx:  idx,
	}
}

// Key 返回当前游标位置的 Key
func (it *TreeIterator) Key() int64 {
	if it.currPage == nil {
		return -1 // 或者 panic，视具体需求而定
	}
	// 假设 BPlusTreePage 有通用的 GetKey 接口，或者内部自动判断是 Leaf
	// 注意：迭代器只会停留在 Leaf Page 上
	return it.currPage.GetKey(it.currIdx)
}

// Value 返回当前游标位置的 Value
func (it *TreeIterator) Value() []byte {
	if it.currPage == nil {
		return nil
	}
	return it.currPage.GetValue(it.currIdx)
}

func (it *TreeIterator) Next() bool {
	if it.currPage == nil {
		return false
	}

	it.currIdx++

	if it.currIdx < it.currPage.GetCount() {
		return true
	}

	nextPageId := it.currPage.GetNextPageID()
	
	// 修复 1: 显式类型转换
	it.bpm.UnpinPage(page.PageID(it.currPage.GetPageID()), false)

	if nextPageId == 0 { 
		it.currPage = nil
		return false
	}

	rawPage := it.bpm.FetchPage(page.PageID(nextPageId))
	if rawPage == nil {
		it.currPage = nil
		return false
	}

	it.currPage = page.NewBPlusTreePage(rawPage)
	it.currIdx = 0 

	return true
}

// Close 关闭迭代器
func (it *TreeIterator) Close() {
	if it.currPage != nil {
        // 修复 2: 显式类型转换
		it.bpm.UnpinPage(page.PageID(it.currPage.GetPageID()), false)
		it.currPage = nil
	}
}

// IsValid 检查迭代器当前是否指向有效数据
func (it *TreeIterator) IsValid() bool {
	return it.currPage != nil
}