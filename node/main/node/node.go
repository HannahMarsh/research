package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"net/http"
	"node/main/cq"
	"sync"
	"time"
)

type OtherNode struct {
	address   string
	data      map[string]map[string][]byte
	dataMutex sync.RWMutex
}

type Node struct {
	Ctx         context.Context
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	otherNodes  []OtherNode
	id          int
	cq          *cq.CQ
}

func CreateNewNode(id int, address string, maxMemMbs int, maxMemoryPolicy string, otherNodes []string) *Node {
	ctx := context.Background()
	c := new(Node)
	c.id = int(id)
	c.Ctx = ctx
	c.otherNodes = make([]OtherNode, len(otherNodes))
	c.cq = cq.NewConcurrentQueue(20_000)

	for node, addr := range otherNodes {
		c.otherNodes[node] = OtherNode{
			address: addr,
		}
	}

	opts := &redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
		//PoolSize: 200, // set the pool size to 100
		//MinIdleConns: 30,                     // maintain at least 10 idle connections
		//IdleTimeout:  10 * time.Second, // timeout for idle connections
		//DialTimeout:  10 * time.Second, // timeout for connecting
		//ReadTimeout:  1 * time.Second,  // timeout for reads
		//WriteTimeout: 1 * time.Second,  // timeout for writes
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

	c.StartTopKeysUpdateTask(1 * time.Second)
	return c
}

func (c *Node) Done() {
	if err := c.redisClient.Close(); err != nil {
		log.Printf("Failed to close Redis client: %v", err)
	}
	c.Ctx.Done()
}

func (c *Node) StartTopKeysUpdateTask(updateInterval time.Duration) {
	ticker := time.NewTicker(updateInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !c.IsFailed() {
					go c.SendUpdateToBackUpNodes()
				}
			case <-c.Ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

const (
	numToSend int = 1000
)

func (c *Node) SendUpdateToBackUpNodes() {
	m := c.cq.GetTop(numToSend)

	for node, data := range m {

		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			return
		}

		// Create a new POST request with JSON body
		req, err := http.NewRequest("POST", c.otherNodes[node].address, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Println("Error creating request:", err)
			return
		}

		// Set Content-Type header
		req.Header.Set("Content-Type", "application/json")

		// Create an HTTP client and send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Error sending request:", err)
			return
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				panic(err)
			}
		}(resp.Body)

		// Read and print the response body
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			return
		}
	}
}

func (c *Node) ReceiveUpdate(data map[string]map[string][]byte, node int) {
	c.otherNodes[node].dataMutex.Lock()
	c.otherNodes[node].data = data
	c.otherNodes[node].dataMutex.Unlock()
}

func (c *Node) Set(key string, value map[string][]byte, backupNode int) (error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return err, 0
	}
	size_ := c.Size(c.Ctx)

	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	c.cq.Set(key, cq.Data{Key: key, Value: value, PrimaryNode: c.id, BackUpNode: backupNode})

	_, err = c.redisClient.Set(c.Ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}

func (c *Node) Get(key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return nil, err, 0
	}
	size_ := c.Size(c.Ctx)

	str := c.redisClient.Get(c.Ctx, key)
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

	c.cq.Get(key)

	// If no specific fields are requested, return the full data
	if len(fields) == 0 {
		return data, nil, size_
	}

	// Extract only the requested fields
	result := make(map[string][]byte)
	for _, field := range fields {
		if value2, ok := data[field]; ok {
			result[field] = value2
		}
	}
	return result, nil, size_
}

func (c *Node) GetBackUp(key string, fields []string, node int) (map[string][]byte, error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return nil, err, 0
	}
	size_ := c.Size(c.Ctx)
	c.otherNodes[node].dataMutex.RLock()
	if data, present := c.otherNodes[node].data[key]; present {
		c.otherNodes[node].dataMutex.RUnlock()
		// If no specific fields are requested, return the full data
		if len(fields) == 0 {
			return data, nil, size_
		}

		// Extract only the requested fields
		result := make(map[string][]byte)
		for _, field := range fields {
			if value2, ok := data[field]; ok {
				result[field] = value2
			}
		}
		return result, nil, size_
	} else {
		c.otherNodes[node].dataMutex.RUnlock()
		return nil, redis.Nil, size_
	}
}

func (c *Node) SetBackup(key string, value map[string][]byte, node int) (error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return err, 0
	}
	size_ := c.Size(c.Ctx)

	c.otherNodes[node].dataMutex.Lock()
	c.otherNodes[node].data[key] = value
	c.otherNodes[node].dataMutex.Unlock()
	return nil, size_
}

func (c *Node) IsFailed() bool {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	return c.isFailed
}

func (c *Node) checkFailed() (error, bool) {
	if c.IsFailed() {
		// time.Sleep(10 * time.Second)
		return context.DeadlineExceeded, true
	}
	return nil, false
}

func (c *Node) Size(ctx context.Context) int64 {
	if c.IsFailed() {
		return 0
	}
	size, err := c.redisClient.DBSize(ctx).Result()
	if err != nil {
		//panic(err)
		return 0
	}
	return size
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
	c.isFailed = true
	c.failMutex.Unlock()
	// TODO clear c.cq
}
