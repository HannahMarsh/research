package main

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

type DbWrapper struct {
	session     *gocql.Session
	keyspace    string
	tableName   string
	concurrency chan struct{}
}

func NewDbWrapper(keyspace string, tableName string, maxConcurrency int, hosts ...string) *DbWrapper {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to connect to Cassandra: %v", err)
		//return nil
	}

	kv := &DbWrapper{session: session, keyspace: keyspace, tableName: tableName, concurrency: make(chan struct{}, maxConcurrency)}
	// Create keyspace and table
	if err := kv.CreateKeyspace(keyspace, "SimpleStrategy", 1); err != nil {
		log.Fatalf("Failed to create keyspace: %v", err)
	}
	if err := kv.CreateTable(tableName); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	return kv
}

func (k *DbWrapper) CreateTable(tableName string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (key text PRIMARY KEY, value text);", k.keyspace, tableName)
	return k.session.Query(query).Exec()
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
	k.concurrency <- struct{}{}        // Wait for permission to proceed
	defer func() { <-k.concurrency }() // Release the slot when done

	query := fmt.Sprintf("INSERT INTO %s.%s (key, value) VALUES (?, ?);", k.keyspace, k.tableName)

	if err := k.session.Query(query, key, value).Exec(); err != nil {
		log.Printf("Failed to put key: %v", err)
	}
}

func (k *DbWrapper) Get(key string) (string, bool) {
	k.concurrency <- struct{}{}        // Wait for permission to proceed
	defer func() { <-k.concurrency }() // Release the slot when done

	var value string
	query := fmt.Sprintf("SELECT value FROM %s.%s WHERE key = ? LIMIT 1;", k.keyspace, k.tableName)

	if err := k.session.Query(query, key).Consistency(gocql.One).Scan(&value); err != nil {
		if !errors.Is(gocql.ErrNotFound, err) {
			log.Printf("Failed to get key: %v", err)
		}
		return "", false
	}
	return value, true
}

//func (k *DbWrapper) Delete(key string) {
//	if err := k.session.Query(`DELETE FROM my_table WHERE key = ?`,
//		key).Exec(); err != nil {
//		log.Printf("Failed to delete key: %v", err)
//	}
//}

func (k *DbWrapper) Close() {
	k.session.Close()
}
