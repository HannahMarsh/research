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
	"sync"
	"time"
)

type OtherNode struct {
	address    string
	data       map[string]map[string][]byte
	dataMutex  sync.Map
	backUpData sync.Map
}

type Node struct {
	ctx         context.Context
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	otherNodes  []OtherNode
	id          int
	topKeys     sync.Map // Store this node's top hottest keys

	data []sync.Map
}

func CreateNewNode(id int, address string, maxMemMbs int, maxMemoryPolicy string, otherNodes []string) *Node {
	ctx := context.Background()
	c := new(Node)
	c.id = int(id)
	c.ctx = ctx
	c.otherNodes = make([]OtherNode, len(otherNodes))

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
	c.ctx.Done()
}

func (o *OtherNode) Set(key string, vvv map[string][]byte) {
	v, _ := o.dataMutex.LoadOrStore(key, new(sync.Mutex))
	if vv, ok2 := v.(*sync.Mutex); ok2 {
		vv.Lock()
		defer vv.Unlock()
		o.data[key] = vvv
	} else {
		panic("failed to load or store")
	}
}

func (o *OtherNode) Get(key string) map[string][]byte {
	//v, _ := o.data.LoadOrStore(key, new(value))
	//if vv, ok2 := v.(*value); ok2 {
	//	vvv, _ := vv.GetAndIncrement()
	//	return vvv
	//} else {
	//	panic("failed to load or store")
	//}
	return o.data[key]
}

func (c *Node) UpdateKey(key string, vvv map[string][]byte, node int, accessCount int) {
	k, _ := c.topKeys.LoadOrStore(node, new(sync.Map))
	if keys, ok := k.(*sync.Map); ok {
		v, _ := keys.LoadOrStore(key, new(value))
		if vv, ok2 := v.(*value); ok2 {
			vv.SetValueAndAccessCount(vvv, int64(accessCount))
		} else {
			panic("failed to load or store")
		}
	} else {
		panic("failed to load or store")
	}
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
			case <-c.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Node) SendUpdateToBackUpNodes() {

	k, _ := c.topKeys.LoadOrStore(c.id, new(sync.Map))
	if keys, ok := k.(*sync.Map); ok {
		keys.Range(func(key, v interface{}) bool {
			if vv, ok2 := v.(*value); ok2 {
				// Marshal data into JSON
				data, node := vv.Get()
				jsonData, err := json.Marshal(data)
				if err != nil {
					fmt.Println("Error marshaling JSON:", err)
					return false
				}

				// Create a new POST request with JSON body
				req, err := http.NewRequest("POST", c.otherNodes[node].address, bytes.NewBuffer(jsonData))
				if err != nil {
					fmt.Println("Error creating request:", err)
					return false
				}

				// Set Content-Type header
				req.Header.Set("Content-Type", "application/json")

				// Create an HTTP client and send the request
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					fmt.Println("Error sending request:", err)
					return false
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
					return false
				}
			} else {
				panic("failed to iterate")
			}

			return true // Continue iteration
		})

	} else {
		panic("failed to load or store")
	}

}

func (c *Node) SetTopKey(node int, key string, vvv map[string][]byte, backupNode int) {
	k, _ := c.topKeys.LoadOrStore(node, new(sync.Map))
	if keys, ok := k.(*sync.Map); ok {
		v, _ := keys.LoadOrStore(key, new(value))
		if vv, ok2 := v.(*value); ok2 {
			vv.SetAndIncrement(vvv, backupNode)
		} else {
			panic("failed to load or store")
		}
	} else {
		panic("failed to load or store")
	}
}

func (c *Node) IncrementAccessCount(node int, key string) {
	k, _ := c.topKeys.LoadOrStore(node, new(sync.Map))
	if keys, ok := k.(*sync.Map); ok {
		v, _ := keys.LoadOrStore(key, new(value))
		if vv, ok2 := v.(*value); ok2 {
			vv.increment()
		} else {
			panic("failed to load or store")
		}
	} else {
		panic("failed to load or store")
	}
}

func (c *Node) GetTopKey(node int, key string) (map[string][]byte, int, bool) {
	k, _ := c.topKeys.LoadOrStore(node, new(sync.Map))
	if keys, ok := k.(*sync.Map); ok {
		v, loaded := keys.LoadOrStore(key, new(value))
		if loaded {
			if vv, ok2 := v.(*value); ok2 {
				vvvv, backup := vv.GetAndIncrement()
				return vvvv, backup, true
			} else {
				panic("failed to load or store")
			}
		} else {
			return nil, -1, false
		}
	} else {
		panic("failed to load or store")
	}
}

func (c *Node) Set(key string, value map[string][]byte, backupNode int) (error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return err, 0
	}
	size_ := c.Size(c.ctx)

	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	c.SetTopKey(c.id, key, value, backupNode)

	_, err = c.redisClient.Set(c.ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}

func (c *Node) Get(key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return nil, err, 0
	}
	go c.IncrementAccessCount(c.id, key)
	size_ := c.Size(c.ctx)

	str := c.redisClient.Get(c.ctx, key)
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
	size_ := c.Size(c.ctx)
	if data, _, present := c.GetTopKey(node, key); present {

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
		return nil, redis.Nil, size_
	}
}

func (c *Node) SetBackup(key string, value map[string][]byte, node int) (error, int64) {
	if err, isFailed := c.checkFailed(); isFailed {
		return err, 0
	}
	size_ := c.Size(c.ctx)

	c.SetTopKey(node, key, value, -1)

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
	if c.isFailed {
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
	c.topKeys = sync.Map{}
}
