package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jamesbcook/print"
)

const (
	allDBQuery = `
	SELECT
	name
	FROM master.dbo.sysdatabases
	WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
	`
	columnNames = "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME ROW_COUNT"
)

//FlagOptions set at startup
type FlagOptions struct {
	Host      string
	InputFile string
	OutPut    string
	User      string
	Domain    string
	Password  string
	Port      int
	Threads   int
	TimeOut   int
	RowCount  uint64
	Verbose   bool
}

//ColumnNames from an all database query
type ColumnNames struct {
	TableCatalog string
	TableSchema  string
	TableName    string
	ColumnName   string
	RowCount     uint64
}

func flagSetup() *FlagOptions {
	host := flag.String("host", "", "MSSQL server to connect to")
	inputFile := flag.String("inputFile", "", "MSSQL servers to connect to")
	outPut := flag.String("output", "mssql-pillage-output", "Directory to write results to")
	port := flag.Int("port", 1433, "Port MSSQL is on")
	user := flag.String("user", "", "Username to authenticate as")
	domain := flag.String("domain", "", "Domain to use")
	pass := flag.String("pass", "", "Password for user")
	threads := flag.Int("threads", 1, "Number of threads to use")
	timeOut := flag.Int("timeOut", 30, "Database Time Out")
	rowCount := flag.Uint64("rowCount", 0, "Number of rows the table must exceed before reporting")
	verbose := flag.Bool("v", false, "Verbose output")
	flag.Parse()
	return &FlagOptions{Host: *host, InputFile: *inputFile, OutPut: *outPut,
		Port: *port, User: *user, TimeOut: *timeOut, RowCount: *rowCount,
		Domain: *domain, Password: *pass, Threads: *threads, Verbose: *verbose}
}

func getServers(inputFile string) []string {
	output, err := ioutil.ReadFile(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return strings.Split(string(output), "\n")
}

func readyDir(dir string) {
	err := os.Mkdir(dir, 0775)
	if err != nil {
		//fmt.Printf("%s Exists, would you like to remove it? ", dir)
		print.Warningf("%s Exists, would you like to remove it? ", dir)
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		if strings.ToLower(string([]byte(input)[0])) == "y" {
			os.RemoveAll(dir)
			err = os.Mkdir(dir, 0775)
			if err != nil {
				print.Badln(err)
			}
		} else {
			print.Badln(err)
		}
	}
	err = os.Chdir(dir)
	if err != nil {
		print.Badln(err)
	}
}

func main() {
	fo := flagSetup()
	if fo.Host == "" && fo.InputFile == "" {
		print.Badln("Need server or Input File")
	}
	var connString string
	var hosts []string
	var dir string
	if fo.Host != "" {
		hosts = append(hosts, fo.Host)
	}
	if fo.InputFile != "" {
		hosts = getServers(fo.InputFile)
	}
	if fo.OutPut != "" {
		dir = fo.OutPut
	} else {
		dir = "mssql-pillage-output"
	}
	readyDir(dir)
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if fo.Domain != "" {
			/*
			* ApplicationIntent doesn't look like it affects anything and
			* it is needed for some databases that expect the setting.
			* It also helpes us confirm we request readonly rights.
			 */
			connString = fmt.Sprintf("server=%s;user id=%s\\%s;password=%s;port=%d;ApplicationIntent=ReadOnly;connection timeout=%d",
				host, fo.Domain, fo.User, fo.Password, fo.Port, fo.TimeOut)
		} else {
			connString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;ApplicationIntent=ReadOnly;connection timeout=%d",
				host, fo.User, fo.Password, fo.Port, fo.TimeOut)
		}
		conn, err := connect(connString)
		if err != nil {
			print.Warningln("Open connection failed:", err)
			continue
		}
		defer conn.Close()
		print.Goodln("Connected to Server", host)
		allDB, err := listDB(conn)
		if err != nil {
			print.Warningln("listdb error", err)
			continue
		}
		results := make(map[string][]ColumnNames)
		for _, dbName := range allDB {
			print.Statusln("Query Database", dbName)
			res, err := databaseEnum(conn, dbName)
			if err != nil {
				print.Warningln("Database Enum error:", err)
				continue
			}
			for _, x := range res {
				if fo.Verbose {
					print.Goodln(x.TableName)
				}
				search2 := `select count(*) from `
				search2 += fmt.Sprintf("[%s].[%s].[%s]", dbName, x.TableSchema, x.TableName)
				rowCount, err := getTableCount(conn, search2)
				if err != nil {
					print.Warningln("Get Table Count error", err)
					continue
				}
				if rowCount == uint64(0) || rowCount < fo.RowCount {
					continue
				}
				x.RowCount = rowCount
				results[dbName] = append(results[dbName], x)
			}

		}
		f, err := os.Create(host)
		if err != nil {
			print.Warningln("file create error", err)
			continue
		}
		for dbName, rows := range results {
			for _, row := range rows {
				f.WriteString(fmt.Sprintf("%s %s %s %s %s %d\n", dbName, row.TableCatalog, row.TableSchema, row.TableName, row.ColumnName, row.RowCount))
				if fo.Verbose {
					print.Goodln(dbName, row.TableCatalog, row.TableSchema, row.TableName, row.ColumnName, row.RowCount)
				}
			}
		}
	}
}

func connect(connString string) (*sql.DB, error) {
	return sql.Open("mssql", connString)
}

func listDB(db *sql.DB) ([]string, error) {
	stmt, err := db.Prepare(allDBQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	databases := []string{}
	for rows.Next() {
		var databaseName string
		err = rows.Scan(&databaseName)
		if err != nil {
			return nil, err
		}
		databases = append(databases, databaseName)
	}
	return databases, nil
}

func databaseEnum(db *sql.DB, dbName string) ([]ColumnNames, error) {
	search := `
		SELECT TOP 1000
		TABLE_CATALOG, TABLE_SCHEMA,TABLE_NAME, COLUMN_NAME
		`
	search += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.columns ", dbName)
	search += `
	WHERE LOWER(COLUMN_NAME) LIKE ('%pass%')
	OR LOWER(COLUMN_NAME) LIKE ('%ssn%')
	OR LOWER(COLUMN_NAME) LIKE ('%routing%')
	OR LOWER(COLUMN_NAME) LIKE ('%rtn%')
	OR LOWER(COLUMN_NAME) LIKE ('%address%')
	OR LOWER(COLUMN_NAME) LIKE ('%credit%')
	OR LOWER(COLUMN_NAME) LIKE ('%card%')
	OR LOWER(COLUMN_NAME) LIKE ('%cvv%')
	`
	stmt, err := db.Prepare(search)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ColumnNames
	for rows.Next() {
		tmp := &ColumnNames{}
		var catalog string
		var schema string
		var tableName string
		var columnName string
		err = rows.Scan(&catalog, &schema, &tableName, &columnName)
		if err != nil {
			return nil, err
		}
		tmp.TableCatalog = catalog
		tmp.TableSchema = schema
		tmp.TableName = tableName
		tmp.ColumnName = columnName
		res = append(res, *tmp)
	}
	return res, nil
}

func getTableCount(db *sql.DB, query string) (uint64, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	row := stmt.QueryRow()
	defer stmt.Close()
	var rowCount uint64
	err = row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}
