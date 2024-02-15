package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net/http"
	"strconv"
	"sync"
)

var globalNode *Node

type Node struct {
	ctx         context.Context
	isFailed    bool
	failMutex   sync.Mutex
	redisClient *redis.Client
	id          int
	topKeys     sync.Map // Store this node's top hottest keys
}

type value struct {
	backUpNode    int
	v             []byte
	accesses      int64
	accessesMutex sync.Mutex
	vMutex        sync.Mutex
}

func (v *value) Set(vv []byte, backUpNode int) {
	v.vMutex.Lock()
	defer v.vMutex.Unlock()
	v.v = vv
	v.backUpNode = backUpNode
}

func (v *value) SetAndIncrement(vv []byte, backUpNode int) {
	v.increment()
	v.Set(vv, backUpNode)
}

func (v *value) increment() {
	v.accessesMutex.Lock()
	defer v.accessesMutex.Unlock()
	v.accesses++
}

func (v *value) Get() ([]byte, int) {
	v.vMutex.Lock()
	defer v.vMutex.Unlock()
	return v.v, v.backUpNode
}

func (v *value) GetAndIncrement() ([]byte, int) {
	v.increment()
	return v.Get()
}

func (c *Node) SetTopKey(node int, key string, vvv []byte, backupNode int) {
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

func (c *Node) GetTopKey(node int, key string) ([]byte, int, bool) {
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

func CreateNewNode(idStr string, address string, maxMemMbsStr string, maxMemoryPolicy string) {

	maxMemMbs, err := strconv.ParseInt(maxMemMbsStr, 10, 64)
	if err != nil {
		fmt.Println("Error converting string to int64:", err)
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		fmt.Println("Error converting string to int64:", err)
		return
	}
	ctx := context.Background()
	c := new(Node)
	c.id = int(id)
	c.ctx = ctx

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
	globalNode = c

}

func main() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/newNode", newNodeHandler)
	mux.HandleFunc("/get", handleGet)

	// Start the server with the mux as the handler
	http.ListenAndServe(":8080", mux)
}

// ServeHTTP serves the cache node over HTTP
func newNodeHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	maxMemMbs := r.URL.Query().Get("maxMemMbs")
	maxMemoryPolicy := r.URL.Query().Get("maxMemoryPolicy")
	CreateNewNode(id, "localhost:6379", maxMemMbs, maxMemoryPolicy)
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Node created successfully"))
	if err != nil {
		panic(err)
		return
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	v, err, size := globalNode.Get(kv.Key, kv.Fields)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare the response structure
	response := struct {
		Value map[string][]byte `json:"value"`
		Size  int64             `json:"size"`
	}{
		Value: v,
		Size:  size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key        string            `json:"key"`
		Value      map[string][]byte `json:"value"`
		BackUpNode int               `json:"backupNode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err, size := globalNode.Set(kv.Key, kv.Value, kv.BackUpNode)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare the response structure
	response := struct {
		Size int64 `json:"size"`
	}{
		Size: size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
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

	c.SetTopKey(c.id, key, serializedValue, backupNode)

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
