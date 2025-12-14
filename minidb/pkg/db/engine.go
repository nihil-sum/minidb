package db

import (
	"errors"
	"fmt"
	"io/ioutil"
	"minidb/pkg/buffer"
	"minidb/pkg/storage/disk"
	"minidb/pkg/storage/index"
	"minidb/pkg/storage/page"
	"os"
	"path/filepath"
	"strings"
)

type Engine struct {
	BPM         *buffer.BufferPoolManager
	DiskManager disk.DiskManager
	Catalog     *Catalog
	CurrentDB   string // 每个会话独享的状态
	DataRoot    string
}

func NewEngine(dataRoot string) *Engine {
	if _, err := os.Stat(dataRoot); os.IsNotExist(err) {
		os.Mkdir(dataRoot, 0755)
	}
	return &Engine{
		DataRoot: dataRoot,
	}
}

// NewSession 创建一个新的 Engine 实例用于当前会话
// 共享底层的 BPM、Catalog 和 DiskManager，但隔离 CurrentDB
func (e *Engine) NewSession() *Engine {
	return &Engine{
		BPM:         e.BPM,
		DiskManager: e.DiskManager,
		Catalog:     e.Catalog,
		DataRoot:    e.DataRoot,
		CurrentDB:   "", // 新会话默认未选中数据库
	}
}

func (e *Engine) EnsureDBSelected() error {
	if e.CurrentDB == "" {
		return errors.New("no database selected. use 'use <dbname>' first")
	}
	return nil
}

// ---------------- 数据库操作 ----------------

func (e *Engine) ShowDatabases() ([]string, error) {
	files, err := ioutil.ReadDir(e.DataRoot)
	if err != nil {
		return nil, err
	}
	var dbs []string
	for _, f := range files {
		if f.IsDir() {
			dbs = append(dbs, f.Name())
		}
	}
	return dbs, nil
}

func (e *Engine) CreateDatabase(name string) error {
	path := filepath.Join(e.DataRoot, name)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return fmt.Errorf("database '%s' already exists", name)
	}
	return os.Mkdir(path, 0755)
}

func (e *Engine) DropDatabase(name string) error {
	if e.CurrentDB == name {
		return errors.New("cannot drop the currently open database")
	}
	path := filepath.Join(e.DataRoot, name)
	return os.RemoveAll(path)
}

// UseDatabase 切换当前会话的数据库
func (e *Engine) UseDatabase(name string) error {
	dbPath := filepath.Join(e.DataRoot, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	// 如果这是主引擎初始化（还没有 DiskManager），则初始化资源
	// 如果是会话引擎，DiskManager 已经共享，这里只需要切换 CurrentDB
	// 但为了简单，我们的架构设计是：主引擎负责持有资源，会话引用资源。
	// 这里我们需要处理一种情况：如果是第一次连接数据库（System startup），需要初始化 Global Resources。

	// 在本设计中，我们在 main.go 中统一初始化资源，UseDatabase 仅作为切换 CurrentDB 的检查
	// 但为了兼容之前的逻辑（Lazy Load），我们保留加载逻辑，但要加锁防止并发重置。

	// *为了简化高并发下的逻辑，我们假设资源在 main.go 启动时已加载，或者在这里加锁*
	// 这里简化为：只检查是否存在，然后切换 CurrentDB。
	// 实际的数据加载（BPM等）如果还没做，应该由 Global Engine 做。
	// 鉴于之前代码结构，我们假设 SwitchDatabase 主要是为了切换 Catalog 上下文。

	// *修正*：由于之前的设计是 Lazy Load (NewDiskManager 在 Use 时调用)，多客户端并发时这会有问题。
	// 我们将在 main.go 中改为 Eager Load (预加载)，这里只做切换。

	e.CurrentDB = name
	return nil
}

// InitSystemResources 初始化全局资源（供 main.go 调用）
func (e *Engine) InitSystemResources() error {
	// 这里可以加载默认库或元数据，目前仅作为占位
	return nil
}

func (e *Engine) Close() {
	if e.BPM != nil {
		e.BPM.FlushAllPages()
	}
	if e.Catalog != nil {
		e.Catalog.SaveMeta()
	}
	if e.DiskManager != nil {
		e.DiskManager.Close()
	}
}

// ---------------- 表操作 ----------------

func (e *Engine) CreateTable(tableName string, schema string) error {
	if err := e.EnsureDBSelected(); err != nil {
		return err
	}

	tree := index.NewBPlusTree(page.InvalidPageID, e.BPM)
	tree.StartNewTree()

	rootId := tree.GetRootPageId()

	if !e.Catalog.CreateTable(tableName, schema, rootId) {
		return errors.New("table already exists")
	}
	return nil
}

func (e *Engine) Insert(tableName string, key int64, value string) error {
	if err := e.EnsureDBSelected(); err != nil {
		return err
	}

	meta, ok := e.Catalog.GetTable(tableName)
	if !ok {
		return fmt.Errorf("table '%s' not found", tableName)
	}

	tree := index.NewBPlusTree(page.PageID(meta.RootPageId), e.BPM)

	success := tree.Insert(key, []byte(value))
	if !success {
		return errors.New("insert failed (duplicate key?)")
	}

	newRoot := tree.GetRootPageId()
	if newRoot != page.PageID(meta.RootPageId) {
		e.Catalog.UpdateTableRoot(tableName, newRoot)
	}
	return nil
}

func (e *Engine) SelectAll(tableName string) ([]string, error) {
	if err := e.EnsureDBSelected(); err != nil {
		return nil, err
	}

	meta, ok := e.Catalog.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table '%s' not found", tableName)
	}

	tree := index.NewBPlusTree(page.PageID(meta.RootPageId), e.BPM)
	it := tree.Begin()
	if it == nil {
		return []string{}, nil
	}
	defer it.Close()

	var results []string
	for {
		row := fmt.Sprintf("[%d] %s", it.Key(), string(it.Value()))
		results = append(results, row)

		if !it.Next() {
			break
		}
	}
	return results, nil
}

func (e *Engine) SelectById(tableName string, key int64) (string, bool) {
	if err := e.EnsureDBSelected(); err != nil {
		return "", false
	}

	meta, ok := e.Catalog.GetTable(tableName)
	if !ok {
		return "", false
	}

	tree := index.NewBPlusTree(page.PageID(meta.RootPageId), e.BPM)
	val, found := tree.GetValue(key)
	if !found {
		return "", false
	}
	return string(val), true
}

// DescribeTable 现在返回字符串而不是直接打印
func (e *Engine) DescribeTable(tableName string) (string, error) {
	if err := e.EnsureDBSelected(); err != nil {
		return "", err
	}
	meta, ok := e.Catalog.GetTable(tableName)
	if !ok {
		return "", fmt.Errorf("table '%s' not found", tableName)
	}

	var sb strings.Builder
	sb.WriteString("+----------------+----------------------+\n")
	sb.WriteString(fmt.Sprintf("| Table          | %-20s |\n", meta.Name))
	sb.WriteString("+----------------+----------------------+\n")
	sb.WriteString(fmt.Sprintf("| Root Page ID   | %-20d |\n", meta.RootPageId))
	sb.WriteString("| Schema Definition:                    |\n")
	sb.WriteString(fmt.Sprintf("  %s\n", meta.Schema))
	sb.WriteString("+----------------+----------------------+")
	return sb.String(), nil
}
