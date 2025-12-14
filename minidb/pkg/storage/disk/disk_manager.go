package disk

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"minidb/pkg/storage/page"
)

// DiskManager 负责管理磁盘上的数据文件
type DiskManager interface {
	ReadPage(pageID page.PageID, p *page.Page) error
	WritePage(pageID page.PageID, p *page.Page) error
	AllocatePage() page.PageID
	DeallocatePage(pageID page.PageID) // 新增接口
	Close() error
}

type DiskManagerImpl struct {
	dbFile     *os.File
	fileName   string
	nextPageID page.PageID // 追踪下一个可用的 PageID
}

// NewDiskManager 启动时打开或创建数据库文件
func NewDiskManager(dbFileName string) (*DiskManagerImpl, error) {
	// 确保目录存在
	dir := filepath.Dir(dbFileName)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 打开文件：读写模式 | 如果不存在则创建 | 权限 0664
	file, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return nil, err
	}

	// 计算当前文件大小，从而确定 nextPageID
	// 比如文件大小是 8192 (2页)，那么下一个 ID 就是 2 (0, 1 已存在)
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	nPID := page.PageID(fileInfo.Size() / page.PageSize)

	return &DiskManagerImpl{
		dbFile:     file,
		fileName:   dbFileName,
		nextPageID: nPID,
	}, nil
}

// Close 关闭文件句柄
func (d *DiskManagerImpl) Close() error {
	return d.dbFile.Close()
}

// ReadPage 从磁盘读取指定页的数据到内存中
func (d *DiskManagerImpl) ReadPage(pageID page.PageID, p *page.Page) error {
	offset := int64(pageID) * int64(page.PageSize)

	// 1. 移动文件指针
	_, err := d.dbFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	// 2. 读取数据填充到 Page 的 Data 数组中
	// 注意：这里直接读入 p.Data 切片
	bytesRead, err := d.dbFile.Read(p.Data[:])
	if err != nil {
		return err
	}

	if bytesRead < page.PageSize {
		// 这种情况通常意味着文件损坏或读取越界
		return errors.New("read less than a full page")
	}

	return nil
}

// WritePage 将内存中的页数据写入磁盘
func (d *DiskManagerImpl) WritePage(pageID page.PageID, p *page.Page) error {
	offset := int64(pageID) * int64(page.PageSize)

	_, err := d.dbFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = d.dbFile.Write(p.Data[:])
	if err != nil {
		return err
	}

	// 在高可靠性场景下，这里应该调用 d.dbFile.Sync() 确保刷盘
	// 但为了性能，通常由 Checkpoint 机制批量 Sync
	return nil
}

// AllocatePage 分配一个新的页 ID (简单的追加策略)
func (d *DiskManagerImpl) AllocatePage() page.PageID {
	// 这是一个原子操作的简易版
	ret := d.nextPageID
	d.nextPageID++
	return ret
}
func (d *DiskManagerImpl) DeallocatePage(pageID page.PageID) {
	// 在简单的实现中，我们不回收磁盘空间，只是在元数据中标记。
	// 这是一个空操作，防止编译报错。
}
