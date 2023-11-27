package db

import (
	"errors"
	"github.com/gocql/gocql"
	"log"
)

type KVStore struct {
	session *gocql.Session
}

func NewKVStore(keyspace string, hosts ...string) *KVStore {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}

	return &KVStore{session: session}
}

func (k *KVStore) Put(key, value string) {
	if err := k.session.Query(`INSERT INTO your_table (key, value) VALUES (?, ?)`,
		key, value).Exec(); err != nil {
		log.Printf("Failed to put key: %v", err)
	}
}

func (k *KVStore) Get(key string) (string, bool) {
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

func (k *KVStore) Delete(key string) {
	if err := k.session.Query(`DELETE FROM your_table WHERE key = ?`,
		key).Exec(); err != nil {
		log.Printf("Failed to delete key: %v", err)
	}
}

func (k *KVStore) Close() {
	k.session.Close()
}
