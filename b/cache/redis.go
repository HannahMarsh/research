package cache

import (
	bconfig "benchmark/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

type Node struct {
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	id          int
}

type Cache interface {
	Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64)
	Set(ctx context.Context, key string, value map[string][]byte) (error, int64)
}

func NewNode(p bconfig.NodeConfig, ctx context.Context) *Node {
	address := p.Address.Value
	maxMemMbs := p.MaxMemoryMbs.Value
	maxMemoryPolicy := p.MaxMemoryPolicy.Value

	c := new(Node)
	c.id = p.NodeId.Value
	opts := &redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	// Initialize Redis client
	c.redisClient = redis.NewClient(opts)

	// Set max memory
	if err := c.redisClient.ConfigSet(ctx, "maxmemory", fmt.Sprintf("%dmb", maxMemMbs)).Err(); err != nil {
		panic(err)
	}

	// Verify the configuration
	maxMemoryRes, err := c.redisClient.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		panic(err)
	} // maxMemoryRes is a slice of interfaces, where the actual value is at the second position

	maxMemory, ok := maxMemoryRes[1].(string) // Redis config values are typically strings
	if !ok {
		panic("maxmemory is not a string")
	}
	expectedMaxMemory := fmt.Sprintf("%d", maxMemMbs*1024*1024) // Convert megabytes to bytes
	if maxMemory != expectedMaxMemory {
		panic(fmt.Errorf("maxmemory is not set properly: %s != %s bytes", maxMemory, expectedMaxMemory))
	}

	// Set max memory policy
	if err = c.redisClient.ConfigSet(ctx, "maxmemory-policy", maxMemoryPolicy).Err(); err != nil {
		panic(err)
	}

	// Verify the configuration
	maxMemoryPolicyRes, err := c.redisClient.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		panic(err)
	} // maxMemoryPolicyRes is a slice of interfaces, where the actual value is at the second position
	maxMemPolicy, ok := maxMemoryPolicyRes[1].(string)
	if !ok {
		panic("maxmemory-policy is not a string")
	} else if maxMemPolicy != maxMemoryPolicy {
		panic(fmt.Errorf("maxmemory-policy is not set properly: %s != %s", maxMemPolicy, maxMemoryPolicy))
	}

	c.isFailed = false

	if err = c.redisClient.FlushDB(ctx).Err(); err != nil {
		panic(fmt.Errorf("failed to clear cache: %v", err))
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
	c.failMutex.Lock()
	defer c.failMutex.Unlock()

	if c.isFailed {
		//time.Sleep(100 * time.Millisecond)
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
	}
	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	_, err = c.redisClient.Set(ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}
