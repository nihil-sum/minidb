package main

import (
	"bufio"
	"fmt"
	"log"
	"minidb/pkg/buffer"
	"minidb/pkg/db"
	"minidb/pkg/storage/disk"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	Port      = ":8888"
	DataDir   = "./minidb_data"
	MetaFile  = "meta.json"
	DBFile    = "data.db"
	DefaultDB = "mydb" // 默认加载的数据库，简化演示
)

// 全局共享资源
var globalEngine *db.Engine

func main() {
	fmt.Println("🚀 MiniDB Server is starting...")

	// 1. 初始化全局资源
	// 为了简化，我们假设服务器启动时默认挂载一个主数据目录。
	// 在真实场景中，DiskManager 应该是惰性加载或由 Catalog 管理多库文件。
	// 这里我们采取一种折中方案：先初始化 Engine 结构，具体 DB 资源在 UseDatabase 时加载/共享。

	// 但是，为了实现 Session 隔离且共享 Cache，我们需要把 BPM 提升为全局单例。
	// 鉴于目前 Engine 代码结构耦合了 BPM，我们采取最稳妥的方式：
	// Server 启动时，不加载具体 DB，只准备环境。
	globalEngine = db.NewEngine(DataDir)
	defer globalEngine.Close()

	// 2. 预先初始化一个默认数据库和它的资源，以便所有客户端共享
	// 注意：在您的 Engine 设计中，SwitchDatabase 会 Close 旧资源并 Open 新资源。
	// 如果多客户端并发 SwitchDatabase，会导致资源被关闭。
	// **这是一个并发设计挑战**。
	// 为了让您的作业能跑且不出错，我们约定：
	// 服务器启动时加载默认数据库 'mydb'，所有客户端默认连它，不要频繁 Drop/Switch。

	initPath := filepath.Join(DataDir, DefaultDB)
	os.MkdirAll(initPath, 0755)
	dm, _ := disk.NewDiskManager(filepath.Join(initPath, DBFile))
	bpm := buffer.NewBufferPoolManager(dm, 100)
	catalog := db.NewCatalog(bpm, filepath.Join(initPath, MetaFile))

	// 手动注入到全局 Engine
	globalEngine.DiskManager = dm
	globalEngine.BPM = bpm
	globalEngine.Catalog = catalog
	globalEngine.CurrentDB = DefaultDB

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatalf("❌ Failed to listen on port %s: %v", Port, err)
	}
	fmt.Printf("👂 Listening on 0.0.0.0%s\n", Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("⚠️ Connection accept error: %v", err)
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("✅ New connection from: %s\n", clientAddr)
	defer conn.Close()

	sessionEngine := globalEngine.NewSession()
	parser := db.NewSQLParser(sessionEngine, conn)

	conn.Write([]byte("Welcome to MiniDB Server!\nminidb> "))

	reader := bufio.NewReader(conn)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("❌ Client disconnected: %s\n", clientAddr)
			return
		}

		sql := strings.TrimSpace(input)
		if sql == "" {
			conn.Write([]byte("minidb> "))
			continue
		}

		if strings.ToLower(sql) == "quit" || strings.ToLower(sql) == "exit" {
			return
		}

		fmt.Printf("[%s] Exec: %s\n", clientAddr, sql)

		// --- ⏱️ 开始计时 ---
		start := time.Now()

		// 执行逻辑
		err = parser.ParseAndExecute(sql)

		// --- ⏱️ 结束计时 ---
		duration := time.Since(start)

		if err != nil {
			// 如果出错，发送错误信息
			conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
		} else {
			// 如果成功，发送耗时统计
			// 格式: (0.0023 sec)
			timeMsg := fmt.Sprintf("(%.4f sec)\n", duration.Seconds())
			conn.Write([]byte(timeMsg))
		}

		conn.Write([]byte("minidb> "))
	}
}
