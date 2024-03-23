package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"net"
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
	Ctx             context.Context
	Cancel          context.CancelFunc
	isFailed        bool
	failMutex       sync.Mutex
	redisClient     *redis.Client
	otherNodes      []*OtherNode
	id              int
	cq              *cq.CQ
	keysToOtherNode sync.Map
	httpClient      *http.Client
}

func (n *Node) getOtherNode(id int) (_ *OtherNode, index int) {
	//if id != n.id {
	return n.otherNodes[id], id
	//} else if id > n.id {
	//return n.otherNodes[id-1], id - 1
	//} else {
	//panic("Cannot get other node with the same id")
	//return &n.otherNodes[0]
	//}
}

func CreateNewNode(id int, address string, maxMemMbs int, maxMemoryPolicy string, updateInterval float64, otherNodes []string, numUniqueKeys int) *Node {
	log.Printf("Creating new node with id %d: maxMemMbs: %d\n", id, maxMemMbs)

	ctx, cancel := context.WithCancel(context.Background())
	c := new(Node)
	c.id = int(id)
	c.Ctx = ctx
	c.Cancel = cancel
	c.otherNodes = make([]*OtherNode, len(otherNodes))
	c.cq = cq.NewConcurrentQueue(numUniqueKeys)
	c.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          15,
			IdleConnTimeout:       10 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
		},
		Timeout: 15 * time.Second,
	}

	for node, addr := range otherNodes {
		c.otherNodes[node] = &OtherNode{
			address: addr,
			data:    make(map[string]map[string][]byte),
		}
	}

	opts := &redis.Options{
		Addr:         address,
		Password:     "",               // no password set
		DB:           0,                // use default DB
		PoolSize:     200,              // set the pool size to 100
		MinIdleConns: 30,               // maintain at least 10 idle connections
		IdleTimeout:  10 * time.Second, // timeout for idle connections
		DialTimeout:  10 * time.Second, // timeout for connecting
		ReadTimeout:  5 * time.Second,  // timeout for reads
		WriteTimeout: 5 * time.Second,  // timeout for writes
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

	updateInterval *= 2000
	c.StartTopKeysUpdateTask(time.Duration(updateInterval) * time.Millisecond)
	return c
}

func (n *Node) Done() {
	log.Printf("Node %d is shutting down\n", n.id)
	if err := n.redisClient.Close(); err != nil {
		log.Printf("Failed to close Redis client: %v", err)
	}
	n.Cancel()
}

func (n *Node) StartTopKeysUpdateTask(updateInterval time.Duration) {
	log.Printf("Starting top keys update task for node %d\n", n.id)
	ticker := time.NewTicker(updateInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !n.IsFailed() {
					go n.SendUpdateToBackUpNodes()
				}
			case <-n.Ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

const (
	numToSend int = 500
)

func (n *Node) SendUpdateToBackUpNodes() {
	//log.Printf("Sending top %d keys to backup nodes\n", numToSend)

	m := n.cq.GetTop(numToSend)

	var wg sync.WaitGroup

	for node, data := range m {

		if node == n.id {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			type params struct {
				Data   map[string][]byte `json:"data"`
				NodeId int               `json:"nodeId"`
			}

			d := make(map[string][]byte)
			for k, v := range data {
				jsonData, err := json.Marshal(v)
				if err != nil {
					fmt.Println("Error marshaling JSON:", err)
					return
				}
				d[k] = jsonData
			}

			jsonData, err := json.Marshal(params{Data: d, NodeId: n.id})
			if err != nil {
				fmt.Println("Error marshaling JSON:", err)
				return
			}

			// Create a new POST request with JSON body
			otherNode, _ := n.getOtherNode(node)
			reqBody := bytes.NewBuffer(jsonData)
			req, err := http.NewRequest("POST", otherNode.address+"/updateKey", reqBody)
			if err != nil {
				fmt.Println("Error creating request:", err)
				return
			} else {
				log.Printf("Sending update to node %d\n", node)
			}

			// Set Content-Type header
			//req.Header.Set("Content-Type", "application/json")

			// Set the Content-Type header only if there's a payload
			if jsonData != nil {
				req.Header.Set("Content-Type", "application/json")
			}

			// Create an HTTP client and send the request
			client := n.httpClient
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("Error sending request:", err)
				return
			} else {
				log.Printf("Received response from node %d: %d", node, resp.StatusCode)
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
			} else {
				//log.Printf("Response body: %v\n", response)
			}
		}()
	}
	wg.Wait()
}

func (n *Node) ReceiveUpdate(data map[string][]byte, node int) {
	//log.Printf("Received update from node %d\n", node)

	d := make(map[string]map[string][]byte)
	for k, v := range data {
		var m map[string][]byte
		err := json.Unmarshal(v, &m)
		if err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			return
		}
		d[k] = m
	}
	otherNode, index := n.getOtherNode(node)
	otherNode.dataMutex.Lock()
	otherNode.data = d
	for key, _ := range data {
		n.keysToOtherNode.LoadOrStore(key, index)
	}
	otherNode.dataMutex.Unlock()
}

func (n *Node) Set(key string, value map[string][]byte, backupNode int) (error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return err, 0
	}
	size_ := n.Size(n.Ctx)

	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	n.cq.Set(key, value, backupNode)

	_, err = n.redisClient.Set(n.Ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}

func (n *Node) Get(key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return nil, err, 0
	}
	size_ := n.Size(n.Ctx)

	str := n.redisClient.Get(n.Ctx, key)
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

	n.cq.Get(key)

	// If no specific fields are requested, return the full data
	if fields == nil || len(fields) == 0 {
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

func (n *Node) GetBackUp(key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return nil, err, 0
	}
	size_ := n.Size(n.Ctx)

	if i, ok := n.keysToOtherNode.Load(key); ok {
		if index, ok2 := i.(int); ok2 {
			if index < len(n.otherNodes) {
				otherNode := n.otherNodes[index]
				otherNode.dataMutex.RLock()
				defer otherNode.dataMutex.RUnlock()
				if data, present := otherNode.data[key]; present {
					if len(fields) == 0 { // If no specific fields are requested, return the full data
						return data, nil, size_
					} else { // Extract only the requested fields
						result := make(map[string][]byte)
						for _, field := range fields {
							if value2, ok3 := data[field]; ok3 {
								result[field] = value2
							}
						}
						return result, nil, size_
					}
				} else {
					return nil, redis.Nil, size_
				}
			} else {
				panic("Othernode index out of bounds")
			}
		} else {
			panic("Error loading key to other node index")
		}
	} else {
		// panic("backup data has not yet been stored for this key")
		return nil, redis.Nil, size_
	}
}

func (n *Node) SetBackup(key string, value map[string][]byte, backUpNode int) (error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return err, 0
	}
	size_ := n.Size(n.Ctx)

	i, _ := n.keysToOtherNode.LoadOrStore(key, backUpNode)

	if index, ok2 := i.(int); ok2 {
		if index < len(n.otherNodes) {
			otherNode := n.otherNodes[index]
			otherNode.dataMutex.Lock()
			defer otherNode.dataMutex.Unlock()
			otherNode.data[key] = value
			return nil, size_
		} else {
			panic("Othernode index out of bounds")
		}
	} else {
		panic("Error loading key to other node index")
	}
}

func (n *Node) IsFailed() bool {
	n.failMutex.Lock()
	defer n.failMutex.Unlock()
	return n.isFailed
}

func (n *Node) checkFailed() (error, bool) {
	if n.IsFailed() {
		// time.Sleep(10 * time.Second)
		return context.DeadlineExceeded, true
	}
	return nil, false
}

func (n *Node) Size(ctx context.Context) int64 {
	if n.IsFailed() {
		return 0
	}
	size, err := n.redisClient.DBSize(ctx).Result()
	if err != nil {
		//panic(err)
		return 0
	}
	return size
}

func (n *Node) Recover(ctx context.Context) {
	n.failMutex.Lock()
	defer n.failMutex.Unlock()
	n.isFailed = false

	// clear the cache to simulate an empty state
	err := n.redisClient.FlushDB(ctx).Err()
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
	}
}

func (n *Node) Fail() {
	n.failMutex.Lock()
	n.isFailed = true
	n.failMutex.Unlock()
	// TODO clear c.cq
}
