package db

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// SQLParser 负责解析 SQL 并调用 Engine 执行
type SQLParser struct {
	Engine *Engine
	Output io.Writer // 输出目标（客户端连接）
}

func NewSQLParser(engine *Engine, output io.Writer) *SQLParser {
	return &SQLParser{Engine: engine, Output: output}
}

var (
	reShowDB      = regexp.MustCompile(`(?i)^show\s+databases$`)
	reCreateDB    = regexp.MustCompile(`(?i)^create\s+database\s+(\w+)$`)
	reDropDB      = regexp.MustCompile(`(?i)^drop\s+database\s+(\w+)$`)
	reUseDB       = regexp.MustCompile(`(?i)^use\s+(\w+)$`)
	reShowTables  = regexp.MustCompile(`(?i)^show\s+tables$`)
	reCreateTable = regexp.MustCompile(`(?i)^create\s+table\s+(\w+)\s*\((.+)\)$`)
	reDropTable   = regexp.MustCompile(`(?i)^drop\s+table\s+(\w+)$`)
	reDescribe    = regexp.MustCompile(`(?i)^describe\s+(\w+)$`)
	reInsert      = regexp.MustCompile(`(?i)^insert\s+into\s+(\w+)\s+values\s*\((.+)\)$`)
	reSelect      = regexp.MustCompile(`(?i)^select\s+\*\s+from\s+(\w+)(?:\s+where\s+(.+))?$`)
	reHelp        = regexp.MustCompile(`(?i)^help$`)
)

// ParseAndExecute 解析输入的 SQL 字符串并执行相应逻辑
func (p *SQLParser) ParseAndExecute(sql string) error {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")

	switch {
	case reHelp.MatchString(sql):
		p.printHelp()
		return nil

	case reShowDB.MatchString(sql):
		return p.handleShowDB()

	case reCreateDB.MatchString(sql):
		matches := reCreateDB.FindStringSubmatch(sql)
		if err := p.Engine.CreateDatabase(matches[1]); err != nil {
			return err
		}
		fmt.Fprintln(p.Output, "Database created.")
		return nil

	case reDropDB.MatchString(sql):
		matches := reDropDB.FindStringSubmatch(sql)
		if err := p.Engine.DropDatabase(matches[1]); err != nil {
			return err
		}
		fmt.Fprintln(p.Output, "Database dropped.")
		return nil

	case reUseDB.MatchString(sql):
		matches := reUseDB.FindStringSubmatch(sql)
		return p.handleUseDB(matches[1])

	case reShowTables.MatchString(sql):
		return p.handleShowTables()

	case reCreateTable.MatchString(sql):
		matches := reCreateTable.FindStringSubmatch(sql)
		return p.handleCreateTable(matches[1], matches[2])

	case reDescribe.MatchString(sql):
		matches := reDescribe.FindStringSubmatch(sql)
		res, err := p.Engine.DescribeTable(matches[1])
		if err != nil {
			return err
		}
		fmt.Fprintln(p.Output, res)
		return nil

	case reDropTable.MatchString(sql):
		matches := reDropTable.FindStringSubmatch(sql)
		return p.handleDropTable(matches[1])

	case reInsert.MatchString(sql):
		matches := reInsert.FindStringSubmatch(sql)
		return p.handleInsert(matches[1], matches[2])

	case reSelect.MatchString(sql):
		matches := reSelect.FindStringSubmatch(sql)
		tableName := matches[1]
		condition := ""
		if len(matches) > 2 {
			condition = matches[2]
		}
		return p.handleSelect(tableName, condition)

	default:
		return fmt.Errorf("syntax error or unknown command: %s", sql)
	}
}

// --- Handler 实现 ---

func (p *SQLParser) printHelp() {
	fmt.Fprintln(p.Output, "--- MiniDB Help ---")
	fmt.Fprintln(p.Output, "1.  show databases;")
	fmt.Fprintln(p.Output, "2.  create database <name>;")
	fmt.Fprintln(p.Output, "3.  drop database <name>;")
	fmt.Fprintln(p.Output, "4.  use <name>;")
	fmt.Fprintln(p.Output, "5.  show tables;")
	fmt.Fprintln(p.Output, "6.  create table <name> (<col> <type>, ...);")
	fmt.Fprintln(p.Output, "7.  describe <table>;")
	fmt.Fprintln(p.Output, "8.  insert into <table> values (<id>, <data...>);")
	fmt.Fprintln(p.Output, "9.  select * from <table> [where id = <val>];")
	fmt.Fprintln(p.Output, "10. drop table <table>;")
}

func (p *SQLParser) handleShowDB() error {
	dbs, err := p.Engine.ShowDatabases()
	if err != nil {
		return err
	}
	fmt.Fprintln(p.Output, "Databases:")
	for _, d := range dbs {
		fmt.Fprintln(p.Output, "- "+d)
	}
	return nil
}

func (p *SQLParser) handleUseDB(name string) error {
	if err := p.Engine.UseDatabase(name); err != nil {
		return err
	}
	fmt.Fprintf(p.Output, "Database changed to '%s'.\n", name)
	return nil
}

func (p *SQLParser) handleShowTables() error {
	if err := p.Engine.EnsureDBSelected(); err != nil {
		return err
	}
	tables := p.Engine.Catalog.ListTables()
	fmt.Fprintf(p.Output, "Tables_in_%s:\n", p.Engine.CurrentDB)
	for _, t := range tables {
		fmt.Fprintln(p.Output, "- "+t)
	}
	return nil
}

func (p *SQLParser) handleCreateTable(tableName, colsDef string) error {
	if err := p.Engine.CreateTable(tableName, colsDef); err != nil {
		return err
	}
	fmt.Fprintln(p.Output, "Query OK, 0 rows affected.")
	return nil
}

func (p *SQLParser) handleDropTable(tableName string) error {
	if err := p.Engine.EnsureDBSelected(); err != nil {
		return err
	}
	p.Engine.Catalog.DropTable(tableName)
	fmt.Fprintln(p.Output, "Query OK, 0 rows affected.")
	return nil
}

func (p *SQLParser) handleInsert(tableName, valuesStr string) error {
	parts := strings.Split(valuesStr, ",")
	if len(parts) < 1 {
		return fmt.Errorf("insert values cannot be empty")
	}

	keyStr := strings.TrimSpace(parts[0])
	key, err := strconv.ParseInt(keyStr, 10, 64)
	if err != nil {
		return fmt.Errorf("primary key (first value) must be an integer: %v", err)
	}

	var valParts []string
	for _, v := range parts[1:] {
		cleanVal := strings.Trim(strings.TrimSpace(v), "'\"")
		valParts = append(valParts, cleanVal)
	}
	valStr := strings.Join(valParts, ",")
	if len(valParts) == 0 {
		valStr = " "
	}

	if err := p.Engine.Insert(tableName, int64(key), valStr); err != nil {
		return err
	}
	fmt.Fprintln(p.Output, "Query OK, 1 row affected.")
	return nil
}

func (p *SQLParser) handleSelect(tableName, condition string) error {
	if condition == "" {
		rows, err := p.Engine.SelectAll(tableName)
		if err != nil {
			return err
		}

		fmt.Fprintf(p.Output, "--- %s ---\n", tableName)
		for _, r := range rows {
			fmt.Fprintln(p.Output, r)
		}
		fmt.Fprintf(p.Output, "(%d rows)\n", len(rows))
		return nil
	}

	reWhere := regexp.MustCompile(`(?i)(\w+)\s*=\s*(.+)`)
	matches := reWhere.FindStringSubmatch(condition)
	if len(matches) < 3 {
		return fmt.Errorf("unsupported where clause")
	}

	colName := matches[1]
	valStr := strings.TrimSpace(matches[2])

	if strings.ToLower(colName) == "id" {
		key, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("id must be integer")
		}
		val, found := p.Engine.SelectById(tableName, key)
		if !found {
			fmt.Fprintln(p.Output, "Empty set.")
		} else {
			fmt.Fprintf(p.Output, "--- %s ---\n", tableName)
			fmt.Fprintf(p.Output, "[%d] %s\n", key, val)
			fmt.Fprintln(p.Output, "(1 row)")
		}
		return nil
	}

	return fmt.Errorf("currently only supports filtering by ID")
}
