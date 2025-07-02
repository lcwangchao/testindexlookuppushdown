package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
)

const createTableSQLTemplate = `CREATE TABLE %s (
	id_int bigint NOT NULL,
	id_str varchar(512) NOT NULL,
    k_int bigint NOT NULL,
    k_str varchar(512) NOT NULL,
    d text NOT NULL,
    PRIMARY KEY(id_int, id_str) CLUSTERED
)`

const addIndexSQLTemplate = "ALTER TABLE %s ADD INDEX k (k_int, k_str)"

var (
	dbName    = flag.String("db", "idxlookup", "database name")
	host      = flag.String("host", "127.0.0.1", "host name")
	port      = flag.Int("port", 4000, "TiDB port")
	user      = flag.String("user", "root", "user name")
	threads   = flag.Int("threads", 32, "number of threads to prepare data")
	batch     = flag.Int("batch", 100, "rows per batch in insert statements")
	tableRows = flag.Int("table-rows", 200000000, "number of rows per table")
)

func init() {
	flag.Parse()
}

func MustExec(db *sql.DB, sql string) {
	_, err := db.Exec(sql)
	if err != nil {
		panic(fmt.Sprintf("Failed to execute SQL: %s, error: %v", sql, err))
	}
}

type Job struct {
	suffix   int
	progress atomic.Int64
}

func (j *Job) randStr(size int) string {
	bs := make([]rune, size)
	for i := 0; i < size; i++ {
		bs[i] = 'A' + rune(rand.Intn(26))
	}
	return string(bs)
}

func (j *Job) dataPoint(i int) string {
	distLevelFactor := (i / 1000000) % 10
	pkStrSizeFactor := (i / 10000000) % 10

	idInt := int64((i%(1<<distLevelFactor))*(*tableRows) + i)
	return fmt.Sprintf(
		"(%d, '%s', %d, '%s', '%s')",
		idInt,
		j.randStr(1<<pkStrSizeFactor),
		i,
		j.randStr(16),
		j.randStr(256),
	)
}

func (j *Job) Run(db *sql.DB) {
	tblName := fmt.Sprintf("%s.sbtest%d", *dbName, j.suffix)
	fmt.Printf("Creating table %s ... \n", tblName)
	MustExec(db, fmt.Sprintf(createTableSQLTemplate, tblName))
	fmt.Printf("Insert %d rows to table %s ... \n", *tableRows, tblName)
	rowsEachThread := *tableRows / *threads
	if rowsEachThread == 0 {
		rowsEachThread = 1
	}

	var wg sync.WaitGroup
	for thread := 0; thread < *threads; thread++ {
		start := thread * rowsEachThread
		end := start + rowsEachThread
		if end > *tableRows {
			end = *tableRows
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var insertSQL strings.Builder
			for i := start; i < end; i++ {
				if insertSQL.Len() == 0 {
					insertSQL.WriteString("INSERT INTO ")
					insertSQL.WriteString(tblName)
					insertSQL.WriteString(" (id_int, id_str, k_int, k_str, d) VALUES ")
				} else {
					insertSQL.WriteString(", ")
				}
				insertSQL.WriteString(j.dataPoint(i))

				if (i%*batch)+1 == *batch || i == end-1 {
					MustExec(db, insertSQL.String())
					insertSQL.Reset()
					j.progress.Store(int64(i + 1))
				}
			}
		}()
	}

	wg.Wait()
	fmt.Printf("Adding index to %s ...\n", tblName)
	MustExec(db, fmt.Sprintf(addIndexSQLTemplate, tblName))
	policyName := fmt.Sprintf("p%d", j.suffix)
	fmt.Printf("Set placement %s for %s ...\n", policyName, tblName)
	MustExec(db, fmt.Sprintf("ALTER TABLE %s PLACEMENT POLICY = `%s`", tblName, policyName))
	fmt.Printf("Done for table %s\n", tblName)
}

func main() {
	dsn := fmt.Sprintf("%s:@tcp(%s:%d)/test", *user, *host, *port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	fmt.Println("Creating database ...")
	MustExec(db, "CREATE DATABASE IF NOT EXISTS "+*dbName)
	fmt.Println("Creating placement policy ...")
	MustExec(db, "CREATE PLACEMENT POLICY IF NOT EXISTS `p0` LEADER_CONSTRAINTS=\"[+host=tikv-0]\" FOLLOWERS=2")
	MustExec(db, "CREATE PLACEMENT POLICY IF NOT EXISTS `p1` LEADER_CONSTRAINTS=\"[+host=tikv-1]\" FOLLOWERS=2")
	MustExec(db, "CREATE PLACEMENT POLICY IF NOT EXISTS `p2` LEADER_CONSTRAINTS=\"[+host=tikv-2]\" FOLLOWERS=2")

	job := &Job{}
	job.Run(db)
}
