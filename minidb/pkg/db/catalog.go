package db

import (
	"encoding/json"
	"minidb/pkg/buffer"
	"minidb/pkg/storage/page" // 引入 page 包
	"os"
	"sync"
)

// TableMeta 定义表的元数据
type TableMeta struct {
	Name       string
	RootPageId int32 // 为了 JSON 序列化方便，这里存 int32，使用时转 PageID
	Schema     string
}

type Catalog struct {
	Tables   map[string]*TableMeta
	BPM      *buffer.BufferPoolManager
	MetaFile string
	mu       sync.RWMutex
}

func NewCatalog(bpm *buffer.BufferPoolManager, metaFile string) *Catalog {
	c := &Catalog{
		Tables:   make(map[string]*TableMeta),
		BPM:      bpm,
		MetaFile: metaFile,
	}
	c.LoadMeta()
	return c
}

func (c *Catalog) LoadMeta() {
	file, err := os.Open(c.MetaFile)
	if err != nil {
		return
	}
	defer file.Close()
	json.NewDecoder(file).Decode(&c.Tables)
}

func (c *Catalog) SaveMeta() {
	file, err := os.Create(c.MetaFile)
	if err != nil {
		// 在实际生产中应处理错误，这里简单 panic 或打印
		return
	}
	defer file.Close()
	json.NewEncoder(file).Encode(c.Tables)
}

// CreateTable 注册新表
func (c *Catalog) CreateTable(name string, schema string, initialRootId page.PageID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.Tables[name]; exists {
		return false
	}
	c.Tables[name] = &TableMeta{
		Name:       name,
		RootPageId: int32(initialRootId), // 转换存储
		Schema:     schema,
	}
	c.SaveMeta()
	return true
}

func (c *Catalog) GetTable(name string) (*TableMeta, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	meta, ok := c.Tables[name]
	return meta, ok
}

// HasTable 检查表是否存在 (修复 main.go 中的报错)
func (c *Catalog) HasTable(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.Tables[name]
	return ok
}

func (c *Catalog) UpdateTableRoot(name string, newRootId page.PageID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if table, ok := c.Tables[name]; ok {
		table.RootPageId = int32(newRootId)
		c.SaveMeta()
	}
}

func (c *Catalog) DropTable(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Tables, name)
	c.SaveMeta()
}

func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.Tables))
	for name := range c.Tables {
		names = append(names, name)
	}
	return names
}
