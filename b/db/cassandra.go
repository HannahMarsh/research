// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	bconfig "benchmark/config"
	"benchmark/util"
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// cassandra properties
const (
	cassandraCluster     = "cassandra.cluster"
	cassandraKeyspace    = "cassandra.keyspace"
	cassandraConnections = "cassandra.connections"
	cassandraUsername    = "cassandra.username"
	cassandraPassword    = "cassandra.password"

	cassandraUsernameDefault    = "cassandra"
	cassandraPasswordDefault    = "cassandra"
	cassandraClusterDefault     = "127.0.0.1:9042"
	cassandraKeyspaceDefault    = "test"
	cassandraConnectionsDefault = 2 // refer to https://github.com/gocql/gocql/blob/master/cluster.go#L52
)

// DBCreator creates a database layer.
type DBCreator interface {
	Create(p *bconfig.Config) (DB, error)
}

// DB is the layer to access the database to be benchmarked.
type DB interface {
	// Close closes the database layer.
	Close() error

	// InitThread initializes the state associated to the goroutine worker.
	// The Returned context will be passed to the following usage.
	InitThread(ctx context.Context, threadID int, threadCount int) context.Context

	// CleanupThread cleans up the state when the worker finished.
	CleanupThread(ctx context.Context)

	// Read reads a record from the database and returns a map of each field/value pair.
	// table: The name of the table.
	// key: The record key of the record to read.
	// fields: The list of fields to read, nil|empty for reading all.
	Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error)

	// Scan scans records from the database.
	// table: The name of the table.
	// startKey: The first record key to read.
	// count: The number of records to read.
	// fields: The list of fields to read, nil|empty for reading all.
	Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error)

	// Update updates a record in the database. Any field/value pairs will be written into the
	// database or overwritten the existing values with the same field name.
	// table: The name of the table.
	// key: The record key of the record to update.
	// values: A map of field/value pairs to update in the record.
	Update(ctx context.Context, table string, key string, values map[string][]byte) error

	// Insert inserts a record in the database. Any field/value pairs will be written into the
	// database.
	// table: The name of the table.
	// key: The record key of the record to insert.
	// values: A map of field/value pairs to insert in the record.
	Insert(ctx context.Context, table string, key string, values map[string][]byte) error

	// Delete deletes a record from the database.
	// table: The name of the table.
	// key: The record key of the record to delete.
	Delete(ctx context.Context, table string, key string) error
}

type BatchDB interface {
	// BatchInsert inserts batch records in the database.
	// table: The name of the table.
	// keys: The keys of batch records.
	// values: The values of batch records.
	BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error

	// BatchRead reads records from the database.
	// table: The name of the table.
	// keys: The keys of records to read.
	// fields: The list of fields to read, nil|empty for reading all.
	BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error)

	// BatchUpdate updates records in the database.
	// table: The name of table.
	// keys: The keys of records to update.
	// values: The values of records to update.
	BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error

	// BatchDelete deletes records from the database.
	// table: The name of the table.
	// keys: The keys of the records to delete.
	BatchDelete(ctx context.Context, table string, keys []string) error
}

// AnalyzeDB is the interface for the DB that can perform an analysis on given table.
type AnalyzeDB interface {
	// Analyze performs a key distribution analysis for the table.
	// table: The name of the table.
	Analyze(ctx context.Context, table string) error
}

type CassandraDB struct {
	p       *bconfig.Config
	session *gocql.Session
	verbose bool

	bufPool  *util.BufPool
	keySpace string

	fieldNames []string
}

func NewDatabase(p *bconfig.Config) (*CassandraDB, error) {
	d := new(CassandraDB)
	d.p = p

	hosts := strings.Split(p.CassandraCluster, ",")

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = p.CassandraKeyspace
	d.keySpace = cluster.Keyspace

	cluster.NumConns = p.CassandraConnections
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum

	username := p.CassandraUsername
	password := p.CassandraPassword
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	d.verbose = p.Verbose
	d.session = session

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *CassandraDB) createTable() error {
	tableName := db.p.TableName

	if db.p.DropData {
		if err := db.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db.keySpace, tableName)).Exec(); err != nil {
			return err
		}
	}

	fieldCount := db.p.FieldCount

	db.fieldNames = make([]string, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		db.fieldNames[i] = fmt.Sprintf("field%d", i)
	}

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (YCSB_KEY VARCHAR PRIMARY KEY", db.keySpace, tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR", i))
	}

	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	err := db.session.Query(buf.String()).Exec()
	return err
}

func (db *CassandraDB) Close() error {
	if db.session == nil {
		return nil
	}

	db.session.Close()
	return nil
}

func (db *CassandraDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *CassandraDB) CleanupThread(_ctx context.Context) {

}

func (db *CassandraDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		fields = db.fieldNames
	}

	query = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), db.keySpace, table)

	if db.verbose {
		fmt.Printf("%s\n", query)
	}

	m := make(map[string][]byte, len(fields))
	dest := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		v := new([]byte)
		dest[i] = v
	}

	err := db.session.Query(query, key).WithContext(ctx).Scan(dest...)
	if err == gocql.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	for i, v := range dest {
		m[fields[i]] = *v.(*[]byte)
	}

	return m, nil
}

func (db *CassandraDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *CassandraDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	err := db.session.Query(query, args...).WithContext(ctx).Exec()
	return err
}

func (db *CassandraDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("UPDATE ")
	buf.WriteString(fmt.Sprintf("%s.%s", db.keySpace, table))
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	args = append(args, key)

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *CassandraDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT INTO ")
	buf.WriteString(fmt.Sprintf("%s.%s", db.keySpace, table))
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(" ,?")
	}

	buf.WriteByte(')')

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *CassandraDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s.%s WHERE YCSB_KEY = ?`, db.keySpace, table)

	return db.execQuery(ctx, query, key)
}
