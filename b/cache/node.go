package cache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

type CacheNode struct {
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	maxSize     int64
	id          int
}

func NewCacheWrapper(url string, port string, maxSize int64, id int) *CacheNode {
	c := new(CacheNode)
	opts := &redis.Options{
		Addr:     url + ":" + port,
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	// Initialize Redis client
	c.redisClient = redis.NewClient(opts)

	c.isFailed = false
	c.maxSize = maxSize
	c.id = id

	return c
}

func (c *CacheNode) Size(ctx context.Context) (int64, error) {
	if c.isFailed {
		return 0, errors.New("simulated failure - cache node is not available")
	}
	size, err := c.redisClient.DBSize(ctx).Result()
	return size, err
}

func (c *CacheNode) Recover(ctx context.Context) {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = false

	// clear the cache to simulate an empty state
	err := c.redisClient.FlushDB(ctx).Err()
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
	}
}

func (c *CacheNode) Fail() {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = true
}

func (c *CacheNode) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error) {
	if c.isFailed {
		return nil, errors.New("simulated failure - cache node is not available")
	}

	val, err := c.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // Key does not exist, return nil map and no error
	} else if err != nil {
		return nil, err // Return the error encountered when fetching from Redis
	}

	// Deserialize the JSON back into a map
	var data map[string][]byte
	err = json.Unmarshal([]byte(val), &data)
	if err != nil {
		return nil, err // Handle JSON deserialization error
	}

	// If no specific fields are requested, return the full data
	if len(fields) == 0 {
		return data, nil
	}

	// Extract only the requested fields
	result := make(map[string][]byte)
	for _, field := range fields {
		if value, ok := data[field]; ok {
			result[field] = value
		}
	}

	return result, nil
}

func (c *CacheNode) Set(ctx context.Context, key string, value map[string][]byte) error {
	if c.isFailed {
		return errors.New("simulated failure - cache node is not available")
	}
	size, _ := c.Size(ctx)
	if size >= c.maxSize {
		return errors.New("cache is full")
	}
	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err // Handle JSON serialization error
	}

	_, err = c.redisClient.Set(ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err
}
