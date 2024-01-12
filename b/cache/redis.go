package cache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

type Node struct {
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	maxSize     int64
	id          int
}

type Cache interface {
	Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64)
	Set(ctx context.Context, key string, value map[string][]byte) (error, int64)
}

func NewNode(address string, maxSize int64, id int, ctx context.Context) *Node {
	c := new(Node)
	opts := &redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	// Initialize Redis client
	c.redisClient = redis.NewClient(opts)

	c.isFailed = false
	c.maxSize = maxSize
	c.id = id

	err := c.redisClient.FlushDB(ctx).Err()
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
	}

	return c
}

func (c *Node) Size(ctx context.Context) (int64, error) {
	if c.isFailed {
		return 0, errors.New("simulated failure - cache node is not available")
	}
	size, err := c.redisClient.DBSize(ctx).Result()
	return size, err
}

func (c *Node) Recover(ctx context.Context) {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = false

	// clear the cache to simulate an empty state
	err := c.redisClient.FlushDB(ctx).Err()
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
	}
}

func (c *Node) Fail() {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = true
}

func (c *Node) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {
	if c.isFailed {
		return nil, errors.New("simulated failure - cache node is not available"), 0
	}

	size_ := int64(0)
	if size, err := c.Size(ctx); err == nil {
		size_ = size
	}

	str := c.redisClient.Get(ctx, key)
	val, err := str.Result()
	if err != nil {
		return nil, err, size_ // cache miss happens when err == redis.Nil
	}

	// Deserialize the JSON back into a map
	var data map[string][]byte
	err = json.Unmarshal([]byte(val), &data)
	if err != nil {
		return nil, err, size_ // Handle JSON deserialization error
	}

	// If no specific fields are requested, return the full data
	if len(fields) == 0 {
		return data, nil, size_
	}

	// Extract only the requested fields
	result := make(map[string][]byte)
	for _, field := range fields {
		if value, ok := data[field]; ok {
			result[field] = value
		}
	}

	return result, nil, size_
}

func (c *Node) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {
	if c.isFailed {
		return errors.New("simulated failure - cache node is not available"), 0
	}
	size_ := int64(0)
	if size, err := c.Size(ctx); err == nil {
		size_ = size
		if size >= c.maxSize {
			// cache is full, flush it
			err = c.redisClient.FlushDB(ctx).Err()
			if err != nil {
				log.Printf("Failed to clear cache: %v", err)
				return err, size
			}
		}
	}
	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	_, err = c.redisClient.Set(ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}
