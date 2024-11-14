package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	gofakeit "github.com/brianvoe/gofakeit/v7"
	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/reader"
	duckdb "github.com/marcboeker/go-duckdb"
)

type DBPoolT struct {
	sync.Mutex
	db *sql.DB
}

// DB is the global database connection pool.
var (
	DBPool    DBPoolT
	connector *duckdb.Connector
	err       error
)

type Foo struct {
	Str      string
	Int      int
	Pointer  *int
	Name     string `fake:"{firstname}"`  // Any available function all lowercase
	Sentence string `fake:"{sentence:3}"` // Can call with parameters
	RandStr  string `fake:"{randomstring:[hello,world]}"`
	Number   string `fake:"{number:1,10}"`       // Comma separated for multiple values
	Regex    string `fake:"{regex:[abcdef]{5}}"` // Generate string from regex
	//Map           map[string]int `fakesize:"2"`
	Array         []string `fakesize:"2"`
	ArrayRange    []string `fakesize:"2,6"`
	Bar           Bar
	Skip          *string   `fake:"skip"` // Set to "skip" to not generate data for
	SkipAlt       *string   `fake:"-"`    // Set to "-" to not generate data for
	Created       time.Time // Can take in a fake tag as well as a format tag
	CreatedFormat time.Time `fake:"{year}-{month}-{day}" format:"2006-01-02"`
}

type Bar struct {
	Name       string
	Number     int
	Float      float32
	ArrayRange []string `fakesize:"2,6"`
}

func initDuckDB(dbPath string) error {
	var err error
	var db string
	if dbPath != "" {
		db = dbPath + "?threads=4"
	}
	connector, err = duckdb.NewConnector(db, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'parquet'",
			"LOAD 'parquet'",
		}

		for _, qry := range bootQueries {
			_, err = execer.ExecContext(context.Background(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("connector error : %v\n", err)
		os.Exit(1)
	}

	// Create a new database pool with a maximum of 10 connections.
	DBPool.db = sql.OpenDB(connector)
	if err != nil {
		log.Fatal(err)
	}
	DBPool.db.SetMaxOpenConns(10)
	DBPool.db.SetMaxIdleConns(10)
	return nil
}

func main() {
	initDuckDB("duck.db")
	defer DBPool.db.Close()
	start := time.Now()
	// filepath := "test.json"
	log.Println("start")
	var u *bodkin.Bodkin

	// f, err := os.Open(filepath)
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()
	// s := bufio.NewScanner(f)
	u = bodkin.NewBodkin(bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	if err != nil {
		panic(err)
	}

	for q := 0; q < 10; q++ {
		var f Foo
		err := gofakeit.Struct(&f)
		err = u.Unify(f)
		if err != nil {
			panic(err)
		}
	}
	// f.Close()
	err = u.ExportSchemaFile("temp.bak")
	if err != nil {
		panic(err)
	}
	log.Printf("time to infer schema: %v\n", time.Since(start))

	schema, err := u.ImportSchemaFile("temp.bak")
	if err != nil {
		panic(err)
	}
	log.Printf("union %v\n", schema.String())

	file, err := os.Create("temp.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	filegenStart := time.Now()
	var newLine []byte = []byte{byte('\n')}
	for q := 0; q < 100000; q++ {
		var f Foo
		err := gofakeit.Struct(&f)
		fb, err := json.Marshal(f)
		if err != nil {
			panic(err)
		}
		file.Write(fb)
		file.Write(newLine)
	}
	file.Close()
	log.Printf("file generation took: %v\n", time.Since(filegenStart))
	ff, err := os.Open("temp.json")
	if err != nil {
		panic(err)
	}
	defer ff.Close()
	r, err := reader.NewReader(schema, 0, reader.WithIOReader(ff, reader.DefaultDelimiter), reader.WithChunk(1024*16))
	if err != nil {
		panic(err)
	}

	i := 0
	insertStart := time.Now()
	for r.NextBatch(100) {
		log.Printf("elapsed: %v\n", time.Since(start))
		recs := r.RecordBatch()
		duckOut(schema, recs)
		i++
	}
	log.Printf("time to insert: %v\n", time.Since(insertStart))
	log.Println("records", r.Count(), "batches", i)

	log.Printf("total elapsed: %v\n", time.Since(start))
	log.Println("end")
}

func duckOut(schema *arrow.Schema, recs []arrow.Record) {
	dr := DBPool.db.Driver()
	conn, err := dr.Open("duck.db")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	dba, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		panic(err)
	}
	recReader, err := array.NewRecordReader(schema, recs)
	if err != nil {
		panic(err)
	}
	arrowViewRelease, err := dba.RegisterView(recReader, "arrowrecs")
	if err != nil {
		panic(err)
	}
	defer arrowViewRelease()
	_, err = dba.QueryContext(context.Background(), "select * from t1")
	if err != nil {
		_, err = dba.QueryContext(context.Background(), "create table if not exists t1 as select * from 'arrowrecs'")
		if err != nil {
			panic(err)
		}
	} else {
		_, err = dba.QueryContext(context.Background(), "insert into t1 by name (select * from 'arrowrecs')")
		if err != nil {
			panic(err)
		}
	}
}
