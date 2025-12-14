package index

import (
	"bytes"
	"minidb/pkg/buffer"
	"minidb/pkg/storage/page"
	"sync"
)

type BPlusTree struct {
	bpm        *buffer.BufferPoolManager
	rootPageId page.PageID
	mu         sync.RWMutex
}

func NewBPlusTree(rootPageId page.PageID, bpm *buffer.BufferPoolManager) *BPlusTree {
	return &BPlusTree{
		rootPageId: rootPageId,
		bpm:        bpm,
	}
}

func (tree *BPlusTree) GetRootPageId() page.PageID {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return tree.rootPageId
}

func (tree *BPlusTree) IsEmpty() bool {
	return tree.rootPageId == page.InvalidPageID
}

func (tree *BPlusTree) StartNewTree() {
	p := tree.bpm.NewPage()
	if p == nil {
		panic("failed to new page")
	}
	defer tree.bpm.UnpinPage(p.ID(), true)

	root := page.NewBPlusTreePage(p)
	root.Init(uint32(p.ID()), page.KindLeaf, 0)
	tree.rootPageId = p.ID()
}

func (tree *BPlusTree) GetValue(key int64) ([]byte, bool) {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	if tree.IsEmpty() {
		return nil, false
	}

	leafPage := tree.FindLeafPage(key)
	if leafPage == nil {
		return nil, false
	}
	defer tree.bpm.UnpinPage(leafPage.ID(), false)

	leaf := page.NewBPlusTreePage(leafPage)
	count := leaf.GetCount()
	for i := int32(0); i < count; i++ {
		if leaf.GetKey(i) == key {
			return bytes.TrimRight(leaf.GetValue(i), "\x00"), true
		}
	}
	return nil, false
}

func (tree *BPlusTree) FindLeafPage(key int64) *page.Page {
	if tree.rootPageId == page.InvalidPageID {
		return nil
	}
	currPage := tree.bpm.FetchPage(tree.rootPageId)
	if currPage == nil {
		return nil
	}

	for {
		node := page.NewBPlusTreePage(currPage)
		if node.IsLeaf() {
			return currPage
		}

		count := node.GetCount()
		childPageId := uint32(0)
		found := false

		// Iterate keys to find the appropriate child pointer
		for i := count - 1; i >= 0; i-- {
			if node.GetKey(i) <= key {
				childPageId = node.GetValueAsPageID(i)
				found = true
				break
			}
		}

		// If key is smaller than all keys in this node, go to the first child
		if !found && count > 0 {
			childPageId = node.GetValueAsPageID(0)
		}

		tree.bpm.UnpinPage(currPage.ID(), false)
		currPage = tree.bpm.FetchPage(page.PageID(childPageId))
		if currPage == nil {
			return nil
		}
	}
}

func (tree *BPlusTree) Insert(key int64, val []byte) bool {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.IsEmpty() {
		tree.StartNewTree()
		rootPage := tree.bpm.FetchPage(tree.rootPageId)
		if rootPage == nil {
			return false
		}
		defer tree.bpm.UnpinPage(rootPage.ID(), true)

		rootNode := page.NewBPlusTreePage(rootPage)
		rootNode.InsertLeaf(key, val)
		return true
	}

	leafPageRaw := tree.FindLeafPage(key)
	if leafPageRaw == nil {
		return false
	}
	leafNode := page.NewBPlusTreePage(leafPageRaw)

	if leafNode.IsFull() {
		newPageRaw := tree.bpm.NewPage()
		if newPageRaw == nil {
			tree.bpm.UnpinPage(leafPageRaw.ID(), false)
			return false
		}
		siblingNode := page.NewBPlusTreePage(newPageRaw)
		siblingNode.Init(uint32(newPageRaw.ID()), leafNode.GetPageType(), leafNode.GetParentID())

		siblingNode.SetNextPageID(leafNode.GetNextPageID())
		leafNode.SetNextPageID(siblingNode.GetPageID())

		leafNode.MoveHalfTo(siblingNode)

		if key >= siblingNode.GetKey(0) {
			siblingNode.InsertLeaf(key, val)
		} else {
			leafNode.InsertLeaf(key, val)
		}

		splitKey := siblingNode.GetKey(0)
		tree.InsertIntoParent(leafNode, splitKey, siblingNode)

		tree.bpm.UnpinPage(newPageRaw.ID(), true)
		tree.bpm.UnpinPage(leafPageRaw.ID(), true)
		return true
	} else {
		success := leafNode.InsertLeaf(key, val)
		tree.bpm.UnpinPage(leafPageRaw.ID(), true)
		return success
	}
}

func (tree *BPlusTree) InsertIntoParent(oldNode *page.BPlusTreePage, key int64, newNode *page.BPlusTreePage) {
	if oldNode.GetPageID() == uint32(tree.rootPageId) {
		newRootPageRaw := tree.bpm.NewPage()
		if newRootPageRaw == nil {
			return
		}
		newRoot := page.NewBPlusTreePage(newRootPageRaw)
		newRoot.Init(uint32(newRootPageRaw.ID()), page.KindInternal, 0)

		newRoot.SetCount(2)
		newRoot.SetKey(0, oldNode.GetKey(0))
		newRoot.SetValueAsPageID(0, oldNode.GetPageID())
		newRoot.SetKey(1, key)
		newRoot.SetValueAsPageID(1, newNode.GetPageID())

		tree.rootPageId = newRootPageRaw.ID()
		oldNode.SetParentID(newRoot.GetPageID())
		newNode.SetParentID(newRoot.GetPageID())

		tree.bpm.UnpinPage(newRootPageRaw.ID(), true)
		return
	}

	parentId := oldNode.GetParentID()
	parentPageRaw := tree.bpm.FetchPage(page.PageID(parentId))
	if parentPageRaw == nil {
		return
	}
	parentNode := page.NewBPlusTreePage(parentPageRaw)

	if parentNode.IsFull() {
		newParentSiblingRaw := tree.bpm.NewPage()
		parentSibling := page.NewBPlusTreePage(newParentSiblingRaw)
		parentSibling.Init(uint32(newParentSiblingRaw.ID()), page.KindInternal, parentNode.GetParentID())

		count := parentNode.GetCount()
		splitIdx := count / 2
		moveCount := count - splitIdx

		for i := int32(0); i < moveCount; i++ {
			srcIdx := splitIdx + i
			parentSibling.SetKey(i, parentNode.GetKey(srcIdx))
			parentSibling.SetValueAsPageID(i, parentNode.GetValueAsPageID(srcIdx))

			childPageId := parentNode.GetValueAsPageID(srcIdx)
			childPageRaw := tree.bpm.FetchPage(page.PageID(childPageId))
			if childPageRaw != nil {
				childNode := page.NewBPlusTreePage(childPageRaw)
				childNode.SetParentID(parentSibling.GetPageID())
				tree.bpm.UnpinPage(childPageRaw.ID(), true)
			}
		}
		parentSibling.SetCount(moveCount)
		parentNode.SetCount(splitIdx)

		targetNode := parentNode
		if key >= parentSibling.GetKey(0) {
			targetNode = parentSibling
		}
		tree.insertInternal(targetNode, key, newNode.GetPageID())

		newSplitKey := parentSibling.GetKey(0)
		tree.InsertIntoParent(parentNode, newSplitKey, parentSibling)

		tree.bpm.UnpinPage(newParentSiblingRaw.ID(), true)
	} else {
		tree.insertInternal(parentNode, key, newNode.GetPageID())
	}
	tree.bpm.UnpinPage(parentPageRaw.ID(), true)
}

func (tree *BPlusTree) insertInternal(node *page.BPlusTreePage, key int64, pageID uint32) {
	count := node.GetCount()
	insertIdx := count
	for i := int32(0); i < count; i++ {
		if node.GetKey(i) > key {
			insertIdx = i
			break
		}
	}

	for i := count; i > insertIdx; i-- {
		node.SetKey(i, node.GetKey(i-1))
		node.SetValueAsPageID(i, node.GetValueAsPageID(i-1))
	}

	node.SetKey(insertIdx, key)
	node.SetValueAsPageID(insertIdx, pageID)
	node.SetCount(count + 1)
}

func (tree *BPlusTree) Begin() *TreeIterator {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.rootPageId == page.InvalidPageID {
		return nil
	}

	pageRaw := tree.bpm.FetchPage(tree.rootPageId)
	if pageRaw == nil {
		return nil
	}
	currNode := page.NewBPlusTreePage(pageRaw)

	for !currNode.IsLeaf() {
		childPageId := currNode.GetValueAsPageID(0)
		tree.bpm.UnpinPage(page.PageID(currNode.GetPageID()), false)

		pageRaw = tree.bpm.FetchPage(page.PageID(childPageId))
		if pageRaw == nil {
			return nil
		}
		currNode = page.NewBPlusTreePage(pageRaw)
	}

	return NewTreeIterator(tree.bpm, currNode, 0)
}

func (tree *BPlusTree) Remove(key int64) bool {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.IsEmpty() {
		return false
	}

	leafPageRaw := tree.FindLeafPage(key)
	if leafPageRaw == nil {
		return false
	}
	leafNode := page.NewBPlusTreePage(leafPageRaw)

	// 1. 在叶子中查找并删除 Key
	count := leafNode.GetCount()
	found := false
	for i := int32(0); i < count; i++ {
		if leafNode.GetKey(i) == key {
			leafNode.Remove(i)
			found = true
			break
		}
	}

	if !found {
		tree.bpm.UnpinPage(leafPageRaw.ID(), false)
		return false
	}

	// 2. 删除后检查是否需要调整（Underflow）
	// 如果是根节点，特殊处理
	if leafNode.GetPageID() == uint32(tree.rootPageId) {
		if leafNode.GetCount() == 0 {
			// 树变空了
			tree.rootPageId = page.InvalidPageID
		}
		tree.bpm.UnpinPage(leafPageRaw.ID(), true)
		return true
	}

	// 如果节点元素过少，进行合并或借位
	if leafNode.GetCount() < leafNode.MinDegree() {
		tree.coalesceOrRedistribute(leafNode)
	} else {
		tree.bpm.UnpinPage(leafPageRaw.ID(), true)
	}

	return true
}

// coalesceOrRedistribute 处理 Underflow 的核心逻辑
func (tree *BPlusTree) coalesceOrRedistribute(node *page.BPlusTreePage) {
	// 如果由于递归到了根节点
	if node.GetPageID() == uint32(tree.rootPageId) {
		tree.adjustRoot(node)
		return
	}

	// 获取父节点
	parentId := node.GetParentID()
	parentPageRaw := tree.bpm.FetchPage(page.PageID(parentId))
	parentNode := page.NewBPlusTreePage(parentPageRaw)

	// 找到当前节点在父节点中的索引
	idxInParent := int32(-1)
	parentCount := parentNode.GetCount()
	for i := int32(0); i < parentCount; i++ {
		if parentNode.GetValueAsPageID(i) == node.GetPageID() {
			idxInParent = i
			break
		}
	}

	// 寻找兄弟节点（优先找左兄弟，没有则找右兄弟）
	var siblingPageRaw *page.Page
	var siblingNode *page.BPlusTreePage
	siblingIdx := int32(-1)

	if idxInParent > 0 {
		siblingIdx = idxInParent - 1
		siblingPageRaw = tree.bpm.FetchPage(page.PageID(parentNode.GetValueAsPageID(siblingIdx)))
		siblingNode = page.NewBPlusTreePage(siblingPageRaw)
	} else {
		siblingIdx = idxInParent + 1
		siblingPageRaw = tree.bpm.FetchPage(page.PageID(parentNode.GetValueAsPageID(siblingIdx)))
		siblingNode = page.NewBPlusTreePage(siblingPageRaw)
	}

	// 策略选择：如果兄弟节点有多余的 Key，则借位（Redistribute）；否则合并（Coalesce）
	if siblingNode.GetCount() > siblingNode.MinDegree() {
		// 借位
		isLeftSibling := siblingIdx < idxInParent
		tree.redistribute(siblingNode, node, parentNode, idxInParent, isLeftSibling)
		// 借位完成后，所有涉及的页面都要 Unpin
		tree.bpm.UnpinPage(siblingPageRaw.ID(), true)
		tree.bpm.UnpinPage(parentPageRaw.ID(), true)
		// 注意：node 已经在外部被 Fetch，这里需要在 coalesceOrRedistribute 结束时由调用链 Unpin，
		// 但为了简单，我们在 Remove 里已经 Unpin 了吗？不，如果是 Underflow，Remove 把 Unpin 权交给了这里。
		// 所以我们需要 Unpin node。但 node 是 BPlusTreePage 包装器，我们需要原始 PageID。
		tree.bpm.UnpinPage(page.PageID(node.GetPageID()), true)
	} else {
		// 合并 (Coalesce)
		// 确保将右边的合并到左边，方便逻辑处理
		if siblingIdx < idxInParent {
			// Sibling(Left) + Node(Right)
			tree.coalesce(siblingNode, node, parentNode, idxInParent) // idxInParent 指向 Right
			// Coalesce 内部会处理 node 的删除和 Unpin
			tree.bpm.UnpinPage(siblingPageRaw.ID(), true)
		} else {
			// Node(Left) + Sibling(Right)
			tree.coalesce(node, siblingNode, parentNode, siblingIdx) // siblingIdx 指向 Right
			// Coalesce 内部会处理 sibling 的删除
			tree.bpm.UnpinPage(page.PageID(node.GetPageID()), true)
		}
		// Parent 处理在递归中完成
		tree.bpm.UnpinPage(parentPageRaw.ID(), true)
	}
}

// redistribute 借位逻辑
func (tree *BPlusTree) redistribute(sibling *page.BPlusTreePage, node *page.BPlusTreePage, parent *page.BPlusTreePage, idxInParent int32, isLeftSibling bool) {
	if isLeftSibling {
		// 从左兄弟借最后一个
		// 1. 移动数据
		sibling.MoveLastToFrontOf(node)

		// 2. 更新 Parent 分隔 Key
		// Parent 中分隔 Left 和 Right 的 Key 索引是 idxInParent-1 (如果是 Internal)
		// 或者是 idxInParent (指向 node) ?
		// 在我们的 Internal Node 结构中 (Key[i], Ptr[i]), Ptr[i] 对应的 Key 是 Key[i]。
		// 也就是 Key[i] <= Ptr[i] 的所有值。
		// 当我们修改了 Node(Ptr[i]) 的最小值（因为从左边借了一个更小的），我们需要更新 Key[i]。
		parent.SetKey(idxInParent, node.GetKey(0))

		// 3. 如果是内部节点，移动过来的子节点需要更新 Parent 指针
		if !node.IsLeaf() {
			childId := node.GetValueAsPageID(0)
			childPage := tree.bpm.FetchPage(page.PageID(childId))
			childNode := page.NewBPlusTreePage(childPage)
			childNode.SetParentID(node.GetPageID())
			tree.bpm.UnpinPage(childPage.ID(), true)
		}
	} else {
		// 从右兄弟借第一个
		sibling.MoveFirstToEndOf(node)

		// 更新 Parent 分隔 Key (右兄弟的第一个 Key 变了)
		// 右兄弟的索引是 idxInParent + 1
		parent.SetKey(idxInParent+1, sibling.GetKey(0))

		if !node.IsLeaf() {
			childId := node.GetValueAsPageID(node.GetCount() - 1)
			childPage := tree.bpm.FetchPage(page.PageID(childId))
			childNode := page.NewBPlusTreePage(childPage)
			childNode.SetParentID(node.GetPageID())
			tree.bpm.UnpinPage(childPage.ID(), true)
		}
	}
}

// coalesce 合并逻辑 (Left + Right -> Left)
func (tree *BPlusTree) coalesce(left *page.BPlusTreePage, right *page.BPlusTreePage, parent *page.BPlusTreePage, rightIdxInParent int32) {
	// 1. 移动所有数据从 Right 到 Left
	// 内部节点合并时比较复杂（需要把 Parent 的 Key 拉下来），这里简化为直接移动
	right.MoveAllTo(left, 0)

	// 2. 如果是叶子，维护链表
	if left.IsLeaf() {
		left.SetNextPageID(right.GetNextPageID())
	} else {
		// 如果是内部节点，更新所有移动过来的孩子的父指针
		count := left.GetCount()
		for i := int32(0); i < count; i++ {
			childId := left.GetValueAsPageID(i)
			childPage := tree.bpm.FetchPage(page.PageID(childId))
			childNode := page.NewBPlusTreePage(childPage)
			if childNode.GetParentID() != left.GetPageID() {
				childNode.SetParentID(left.GetPageID())
				tree.bpm.UnpinPage(childPage.ID(), true)
			} else {
				tree.bpm.UnpinPage(childPage.ID(), false)
			}
		}
	}

	// 3. 从父节点删除指向 Right 的指针
	parent.Remove(rightIdxInParent)

	// 4. 释放 Right 页面
	tree.bpm.DeletePage(page.PageID(right.GetPageID()))

	// 5. 递归：如果父节点 Underflow，继续处理
	if parent.GetCount() < parent.MinDegree() {
		tree.coalesceOrRedistribute(parent)
	}
}

// adjustRoot 处理根节点变空或缩减的情况
func (tree *BPlusTree) adjustRoot(oldRoot *page.BPlusTreePage) {
	// 情况 1: 根是叶子，且被清空了
	if oldRoot.IsLeaf() && oldRoot.GetCount() == 0 {
		tree.rootPageId = page.InvalidPageID
		tree.bpm.DeletePage(page.PageID(oldRoot.GetPageID()))
		return
	}

	// 情况 2: 根是内部节点，且只有一个孩子了
	// B+ 树特性：根节点至少要有 2 个孩子，除非它是叶子。
	// 如果根只剩 1 个孩子，这个孩子就变成新的根（树高度减 1）。
	if !oldRoot.IsLeaf() && oldRoot.GetCount() == 1 {
		childId := oldRoot.GetValueAsPageID(0)
		childPage := tree.bpm.FetchPage(page.PageID(childId))
		childNode := page.NewBPlusTreePage(childPage)

		childNode.SetParentID(0) // 新根没有父节点
		tree.rootPageId = childPage.ID()

		tree.bpm.UnpinPage(childPage.ID(), true)
		tree.bpm.DeletePage(page.PageID(oldRoot.GetPageID()))
	} else {
		tree.bpm.UnpinPage(page.PageID(oldRoot.GetPageID()), true)
	}
}
