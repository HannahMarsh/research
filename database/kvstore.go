package database

import "sync"

/**
 * Implements an in-memory key-value store with synchronization primitives for concurrent access.
 * It supports Put(key, value), Get(key) and Delete(key) operations.
 */

type KVStore struct {
	store map[string]string
	mu    sync.Mutex
}

func NewKVStore() *KVStore {
	return &KVStore{store: make(map[string]string)}
}

func (k *KVStore) Put(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.store[key] = value
}

func (k *KVStore) Get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	value, exists := k.store[key]
	return value, exists
}

func (k *KVStore) Delete(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.store, key)
}
