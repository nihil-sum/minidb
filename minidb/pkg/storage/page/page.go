package page

// PageSize 定义一页的大小为 4KB (4096 bytes)
// 这是一个非常标准的数据库页大小，通常和操作系统的内存页大小一致
const PageSize = 4096

// PageID 是页面的唯一标识符
// 使用 int32 是为了方便计算，且 -1 可以用来表示无效页
type PageID int32

const (
	InvalidPageID PageID = -1
)
// Page 结构体代表内存中的一个缓冲页
type Page struct {
	id       PageID
	pinCount int32
	isDirty  bool
	Data     [PageSize]byte // 实际存储数据的字节数组
}

// 下面是一些 Helper 方法，方便后续 Buffer Pool 使用

func (p *Page) ID() PageID {
	return p.id
}

func (p *Page) SetID(id PageID) {
	p.id = id
}

func (p *Page) PinCount() int32 {
	return p.pinCount
}

func (p *Page) IsDirty() bool {
	return p.isDirty
}

func (p *Page) SetPinCount(count int32) {
	p.pinCount = count
}

func (p *Page) SetDirty(dirty bool) {
	p.isDirty = dirty
}

// Clear 将页面数据清空（通常在重用页面时调用）
func (p *Page) Clear() {
    // 这种写法比循环赋值更快
	p.Data = [PageSize]byte{}
}