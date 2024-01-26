package db

import (
	bconfig "benchmark/config"
	"benchmark/util"
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type DB interface {
	Close() error
	InitThread(ctx context.Context, _ int, _ int) context.Context
	CleanupThread(_ctx context.Context)
	Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error)
	Insert(ctx context.Context, table string, key string, values map[string][]byte) error
}

type CassandraDB struct {
	p          *bconfig.Config
	session    *gocql.Session
	verbose    bool
	maxTimeout time.Duration

	bufPool *util.BufPool

	fieldNames []string
}

func NewDatabase(p *bconfig.Config) (*CassandraDB, error) {
	d := new(CassandraDB)
	d.p = p

	hosts := strings.Split(p.Database.CassandraCluster.Value, ",")

	cluster := gocql.NewCluster(hosts...)

	cluster.NumConns = p.Database.CassandraConnections.Value
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum

	if p.Database.PasswordAuthenticator.Value {
		username := p.Database.CassandraUsername.Value
		password := p.Database.CassandraPassword.Value
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	d.verbose = p.Logging.Verbose.Value
	d.session = session
	d.maxTimeout = 15 * time.Millisecond

	d.bufPool = util.NewBufPool()

	if err = d.createKeyspaceIfNotExists(); err != nil {
		return nil, err
	}

	cluster.Keyspace = p.Database.CassandraKeyspace.Value

	if err = d.createTableIfNotExists(); err != nil {
		return nil, err
	}

	if p.Workload.EnableDroppingDataOnStart.Value {
		if err = d.resetTable(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (k *CassandraDB) createKeyspaceIfNotExists() error {
	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': '%s', 'replication_factor': %d};", k.p.Database.CassandraKeyspace.Value, k.p.Database.ReplicationStrategy.Value, k.p.Database.ReplicationFactor.Value)

	if err := k.session.Query(query).Exec(); err != nil {
		log.Printf("Failed to create keyspace %s: %v", k.p.Database.CassandraKeyspace.Value, err)
		return err
	}
	log.Printf("Keyspace %s created successfully", k.p.Database.CassandraKeyspace.Value)
	return nil
}

func (k *CassandraDB) createTableIfNotExists() error {
	if k.p.Workload.EnableDroppingDataOnStart.Value {
		if err := k.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value)).Exec(); err != nil {
			return err
		}
	}

	k.fieldNames = make([]string, int64(k.p.Workload.MaxFields.Value))
	for i := int64(0); i < int64(k.p.Workload.MaxFields.Value); i++ {
		k.fieldNames[i] = fmt.Sprintf("field%d", i)
	}

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (key VARCHAR PRIMARY KEY", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value)
	buf.WriteString(s)

	for i := int64(0); i < int64(k.p.Workload.MaxFields.Value); i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR", i))
	}

	buf.WriteString(");")

	if k.verbose {
		fmt.Println(buf.String())
	}

	err := k.session.Query(buf.String()).Exec()
	return err
}

func (k *CassandraDB) resetTable() error {
	// Drop the table if it exists
	if err := k.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value)).Exec(); err != nil {
		log.Printf("Failed to drop table %s.%s: %v", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value, err)
		return err
	}
	log.Printf("Table %s.%s dropped successfully", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value)

	// Recreate the table
	if err := k.createTableIfNotExists(); err != nil {
		return err
	}

	log.Printf("Table %s.%s created successfully", k.p.Database.CassandraKeyspace.Value, k.p.Database.CassandraTableName.Value)
	return nil
}

func (k *CassandraDB) Close() error {
	if k.session == nil {
		return nil
	}

	k.session.Close()
	return nil
}

func (k *CassandraDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (k *CassandraDB) CleanupThread(_ctx context.Context) {

}

func (k *CassandraDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		fields = k.fieldNames
	}

	//start := time.Now()

	// Create a new context with the timeout
	//ctx, cancel := context.WithTimeout(ctx, k.maxTimeout)
	//defer cancel()

	query = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE key = ?`, strings.Join(fields, ","), k.p.Database.CassandraKeyspace.Value, table)

	if k.verbose {
		fmt.Printf("%s\n", query)
	}

	m := make(map[string][]byte, len(fields))
	dest := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		v := new([]byte)
		dest[i] = v
	}

	err := k.session.Query(query, key).WithContext(ctx).Scan(dest...)
	if err == gocql.ErrNotFound {
		return nil, err
	} else if err != nil {
		return nil, err
	}

	for i, v := range dest {
		m[fields[i]] = *v.(*[]byte)
	}

	//v := time.Since(start).Abs().Seconds()
	//
	//fmt.Printf("Read latency: %f\n", v)
	//
	//if v > k.maxTimeout.Seconds() {
	//	return nil, fmt.Errorf("timeout")
	//}

	return m, nil
}

func (k *CassandraDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if k.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	err := k.session.Query(query, args...).WithContext(ctx).Exec()
	return err
}

func (k *CassandraDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(k.bufPool.Get())
	defer func() {
		k.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT INTO ")
	buf.WriteString(fmt.Sprintf("%s.%s", k.p.Database.CassandraKeyspace.Value, table))
	buf.WriteString(" (key")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(", ")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(" ,?")
	}

	buf.WriteByte(')')

	return k.execQuery(ctx, buf.String(), args...)
}

func (k *CassandraDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s.%s WHERE key = ?`, k.p.Database.CassandraKeyspace.Value, table)

	return k.execQuery(ctx, query, key)
}
