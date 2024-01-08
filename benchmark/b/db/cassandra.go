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
	"benchmark/b/util"
	bconfig "benchmark_config"
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/magiconair/properties"
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

type CassandraDB struct {
	p       *properties.Properties
	session *gocql.Session
	verbose bool

	bufPool  *util.BufPool
	keySpace string

	fieldNames []string
}

func NewDatabase(p *properties.Properties) (*CassandraDB, error) {
	d := new(CassandraDB)
	d.p = p

	hosts := strings.Split(p.GetString(cassandraCluster, cassandraClusterDefault), ",")

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = p.GetString(cassandraKeyspace, cassandraKeyspaceDefault)
	d.keySpace = cluster.Keyspace

	cluster.NumConns = p.GetInt(cassandraConnections, cassandraConnectionsDefault)
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum

	username := p.GetString(cassandraUsername, cassandraUsernameDefault)
	password := p.GetString(cassandraPassword, cassandraPasswordDefault)
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	d.verbose = p.GetBool(bconfig.Verbose, bconfig.VerboseDefault)
	d.session = session

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *CassandraDB) createTable() error {
	tableName := db.p.GetString(bconfig.TableName, bconfig.TableNameDefault)

	if db.p.GetBool(bconfig.DropData, bconfig.DropDataDefault) {
		if err := db.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db.keySpace, tableName)).Exec(); err != nil {
			return err
		}
	}

	fieldCount := db.p.GetInt64(bconfig.FieldCount, bconfig.FieldCountDefault)

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
