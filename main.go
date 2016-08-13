package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
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
	Server      string
	InputFile   string
	OutPut      string
	CustomQuery string
	Database    string
	User        string
	Domain      string
	Password    string
	Port        int
	Threads     int
	Verbose     bool
}

//ColumnNames from an all database query
type ColumnNames struct {
	TableCatalog string
	TableSchema  string
	TableName    string
	ColumnName   string
	RowCount     int
}

//App storage
type App struct {
	db *sql.DB
}

func flagSetup() *FlagOptions {
	server := flag.String("server", "", "MSSQL server to connect to")
	inputFile := flag.String("inputFile", "", "MSSQL servers to connect to")
	outPut := flag.String("output", "mssql-pillage-output", "Directory to write results to")
	customQuery := flag.String("query", "", "Custom query to be sent to the server")
	database := flag.String("database", "", "Database to connect to")
	port := flag.Int("port", 1443, "Port MSSQL is on")
	user := flag.String("user", "", "Username to authenticate as")
	domain := flag.String("domain", "", "Domain to use")
	pass := flag.String("pass", "", "Password for user")
	threads := flag.Int("threads", 1, "Number of threads to use")
	verbose := flag.Bool("v", false, "Verbose output")
	flag.Parse()
	return &FlagOptions{Server: *server, InputFile: *inputFile, OutPut: *outPut,
		Database: *database, Port: *port, User: *user, CustomQuery: *customQuery,
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
		log.Fatal("mkdir", err)
	}
	err = os.Chdir(dir)
	if err != nil {
		log.Fatal("chdir", err)
	}
}

func main() {
	fo := flagSetup()
	if fo.Server == "" && fo.InputFile == "" {
		log.Fatal("Need server or Input File")
	}
	var connString string
	//var database string
	var servers []string
	var dir string
	if fo.OutPut != "" {
		dir = fo.OutPut
	} else {
		dir = "mssql-pillage-output"
	}
	readyDir(dir)
	if fo.Server != "" {
		servers = append(servers, fo.Server)
	}
	//server := fo.Server
	port := 1433
	if fo.InputFile != "" {
		servers = getServers(fo.InputFile)
	}
	for _, server := range servers {
		if server == "" {
			continue
		}
		if fo.Domain != "" {
			connString = fmt.Sprintf("server=%s;user id=%s\\%s;password=%s;port=%d", server, fo.Domain, fo.User, fo.Password, port)
		} else {
			connString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", server, fo.User, fo.Password, port)
		}
		/*
			if fo.Database == "" {
				connString = fmt.Sprintf("server=%s;user id=%s\\%s;password=%s;port=%d", server, domain, user, password, port)
			} else {
				connString = fmt.Sprintf("server=%s;user id=%s\\%s;password=%s;port=%d;database=%s", server, domain, user, password, port, database)
			}
		*/
		conn, err := connect(connString)
		if err != nil {
			//log.Fatal("Open connection failed:", err.Error())
			log.Println("Open connection failed:", err.Error())
			continue
		}
		defer conn.Close()
		/*
			if fo.CustomQuery != "" {
				conn, err := connect(connString)
				err = customExec(conn, fo.CustomQuery)
				if err != nil {
					log.Println(err)
					os.Exit(1)
				}
				os.Exit(0)
			}
		*/
		allDB, err := listDB(conn)
		if err != nil {
			log.Println("listdb error", err)
			continue
		}
		//results := make(map[string][]string)
		results := make(map[string][]ColumnNames)
		for _, dbName := range allDB {
			fmt.Println("Query Database", dbName)
			res, err := databaseEnum(conn, dbName)
			if err != nil {
				log.Println(err)
			}
			for _, x := range res {
				search2 := `select count(*) from `
				search2 += fmt.Sprintf("%s.%s.%s", dbName, x.TableSchema, x.TableName)
				rowCount, err := getTableCount(conn, search2)
				if err != nil {
					log.Println(err)
					continue
				}
				if rowCount == 0 {
					//results[dbName] = res
					continue
				}
				x.RowCount = rowCount
				results[dbName] = append(results[dbName], x)
			}

		}
		f, err := os.Create(server)
		if err != nil {
			log.Println("file creaet error", err)
			continue
		}
		f.WriteString(fmt.Sprintf("%s\n", columnNames))
		fmt.Println(columnNames)
		for dbName, rows := range results {
			for _, row := range rows {
				f.WriteString(fmt.Sprintf("%s %s %s %s %s %d\n", dbName, row.TableCatalog, row.TableSchema, row.TableName, row.ColumnName, row.RowCount))
				fmt.Println(dbName, row.TableCatalog, row.TableSchema, row.TableName, row.ColumnName, row.RowCount)
				// f.WriteString(fmt.Sprintf("%s %s\n", dbName, row))
				// fmt.Println(dbName, row)
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
	//res := []string{}
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
		// resString := fmt.Sprintf("%s %s %s %s", catalog, schema, tableName, columnName)
		// res = append(res, resString)
	}
	return res, nil
	// return res, nil
}

func getTableCount(db *sql.DB, query string) (int, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	row := stmt.QueryRow()
	defer stmt.Close()
	var rowCount int
	err = row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

func customExec(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if cols == nil {
		return nil
	}
	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
		if i != 0 {
			fmt.Print("\t")
		}
		fmt.Print(cols[i])
	}
	fmt.Println()
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for i := 0; i < len(vals); i++ {
			if i != 0 {
				fmt.Print("\t")
			}
			printValue(vals[i].(*interface{}))
		}
		fmt.Println()

	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func printValue(pval *interface{}) {
	switch v := (*pval).(type) {
	case nil:
		fmt.Print("NULL")
	case bool:
		if v {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	case []byte:
		fmt.Print(string(v))
	case time.Time:
		fmt.Print(v.Format("2006-01-02 15:04:05.999"))
	default:
		fmt.Print(v)
	}
}
