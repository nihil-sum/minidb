package page

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPageLayout(t *testing.T) {
	// 1. 创建一个原始的 Page
	rawPage := &Page{}
	
	// 2. 包装成 BPlusTreePage
	node := NewBPlusTreePage(rawPage)

	// 3. 初始化为叶子节点
	node.Init(100, KindLeaf, 0)
	
	assert.Equal(t, uint32(100), node.GetPageID())
	assert.Equal(t, uint32(KindLeaf), node.GetPageType())
	assert.Equal(t, int32(0), node.GetCount())

	// 4. 模拟插入数据
	// 插入第 0 个：Key=1, Val="Hello"
	node.SetKey(0, 1)
	val1 := make([]byte, SizeOfVal)
	copy(val1, []byte("Hello"))
	node.SetValue(0, val1)
	node.SetCount(1)

	// 插入第 1 个：Key=5, Val="World"
	node.SetKey(1, 5)
	val2 := make([]byte, SizeOfVal)
	copy(val2, []byte("World"))
	node.SetValue(1, val2)
	node.SetCount(2)

	// 5. 验证读取
	assert.Equal(t, int64(1), node.GetKey(0))
	assert.Equal(t, int64(5), node.GetKey(1))
	
	readVal1 := node.GetValue(0)
	// 去除尾部的 0 字节以便比较
	assert.Contains(t, string(readVal1), "Hello") 
	
	// 6. 验证修改
	node.SetKey(0, 999)
	assert.Equal(t, int64(999), node.GetKey(0))
}