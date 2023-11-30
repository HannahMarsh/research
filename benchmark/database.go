package benchmark

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

type DbWrapper struct {
	session *gocql.Session
}

func NewDbWrapper(keyspace string, hosts ...string) *DbWrapper {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to connect to Cassandra: %v", err)
		return nil
	}

	kv := &DbWrapper{session: session}
	if keyspace != "" {
		err = kv.CreateKeyspace(keyspace, "SimpleStrategy", 1)
	}
	return kv
}

func (k *DbWrapper) CreateKeyspace(keyspaceName string, replicationStrategy string, replicationFactor int) error {
	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': '%s', 'replication_factor': %d};", keyspaceName, replicationStrategy, replicationFactor)

	if err := k.session.Query(query).Exec(); err != nil {
		log.Printf("Failed to create keyspace %s: %v", keyspaceName, err)
		return err
	}
	log.Printf("Keyspace %s created successfully", keyspaceName)
	return nil
}

func (k *DbWrapper) Put(key, value string) {
	if err := k.session.Query(`INSERT INTO your_table (key, value) VALUES (?, ?)`,
		key, value).Exec(); err != nil {
		log.Printf("Failed to put key: %v", err)
	}
}

func (k *DbWrapper) Get(key string) (string, bool) {
	var value string
	if err := k.session.Query(`SELECT value FROM your_table WHERE key = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&value); err != nil {
		if !errors.Is(gocql.ErrNotFound, err) {
			log.Printf("Failed to get key: %v", err)
		}
		return "", false
	}
	return value, true
}

func (k *DbWrapper) Delete(key string) {
	if err := k.session.Query(`DELETE FROM your_table WHERE key = ?`,
		key).Exec(); err != nil {
		log.Printf("Failed to delete key: %v", err)
	}
}

func (k *DbWrapper) Close() {
	k.session.Close()
}
