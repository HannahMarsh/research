package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spaolacci/murmur3"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()
	c := NewCache(ctx)

	kv := make(map[string]map[string][]byte)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		kv[key] = map[string][]byte{"field1": []byte(value + "-1"), "field2": []byte(value + "-2")}
		time.Sleep(5 * time.Millisecond)
	}
	count := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		c.Set(ctx, key, kv[key])
		count++
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	count = 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := kv[key]
		c.Get(ctx, key, nil)

		var result map[string][]byte

		result, _, _ = c.Get(ctx, key, nil)
		//
		//if resp != nil && resp["value"] != nil {
		//	result = resp["value"]
		//}

		if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", expected) {
			fmt.Printf("key: %s, result: %v, \n\t  expected: %v\n", key, result, expected)
		} else {
			//fmt.Printf("key: %s, matched! result: %v\n", key, result)
		}

		count++
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(25 * time.Second)
}

// CreateArray creates an array of n ints from 0 to n-1.
func CreateArray(n int) []int {
	arr := make([]int, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	return arr
}

func cacheMeasure(start time.Time, key string, nodeIndex int, operationType string, err error, cacheSize int64, isHottest bool) {
	fmt.Printf("Cache measure: %s, %s, %d, %v, %d, %v\n", start, key, nodeIndex, operationType, err, cacheSize, isHottest)
}

func markFailureDetection(start time.Time, key string, nodeIndex int, operationType string) {
	fmt.Printf("Failure detection: %s, %s, %d, %s\n", start, key, nodeIndex, operationType)
}

func markRecoveryDetection(start time.Time, nodeIndex int) {
	fmt.Printf("Recovery detection: %s, %d\n", start, nodeIndex)
}

func nodeFailureMeasure(t time.Time, nodeIndex int, isStart bool) {
	fmt.Printf("Node failure measure: %s, %d, %v\n", t, nodeIndex, isStart)
}

func (c *CacheWrapper) sendRequestToNode(nodeId int, method, endpoint string, payload []byte) (string, int) {
	return sendRequest2(method, fmt.Sprintf("%s/%s", c.nodes[nodeId], endpoint), payload)
}

func sendRequest2(method, url string, payload []byte) (string, int) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return "", -1
	}

	// Set the Content-Type header only if there's a payload
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return "", -1
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err)
		}
	}(resp.Body) // Simplified defer statement

	// Read the entire response body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return "", -1
	}

	if resp.StatusCode == 500 && string(b) != "redis: nil\n" {
		log.Printf("Received response from %s: %v: %s\n", url, resp.StatusCode, resp.Status)
	}

	return string(b), resp.StatusCode
}

type CacheWrapper struct {
	nodes map[int]string
	//nodeRing     *cache.NodeRing
	timers []*time.Timer // Timers for scheduling node failures
	ctx    context.Context
	//memNodes          map[string][]int
	mu                sync.Mutex // Mutex to protect memNodes
	cacheTimeout      time.Duration
	failedNodes       map[int]bool
	numFailDetections map[int]int
	threshhold        int
	nodeOrders        [][]int
}

func permute(nums []int) [][]int {
	var helper func([]int, int)
	var res [][]int

	helper = func(nums []int, n int) {
		if n == 1 {
			tmp := make([]int, len(nums))
			copy(tmp, nums)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(nums, n-1)
				if n%2 == 1 {
					nums[i], nums[n-1] = nums[n-1], nums[i]
				} else {
					nums[0], nums[n-1] = nums[n-1], nums[0]
				}
			}
		}
	}

	helper(nums, len(nums))
	return res
}

func NewCache(ctx context.Context) *CacheWrapper {
	c := &CacheWrapper{
		ctx:               ctx,
		cacheTimeout:      time.Duration(3000) * time.Millisecond,
		failedNodes:       make(map[int]bool),
		numFailDetections: make(map[int]int),
		threshhold:        1200,
		nodeOrders:        permute(CreateArray(2)),
		nodes:             make(map[int]string),
	}

	updateInterval := time.Duration((time.Duration(15)*time.Second).Milliseconds()/100) * time.Millisecond

	c.addNode("http://localhost:8081", 10, "allkeys-lru", 1, updateInterval.Seconds())
	c.addNode("http://localhost:8082", 10, "allkeys-lru", 2, updateInterval.Seconds())

	go c.scheduleCheckForRecoveries(ctx)
	return c
}

func (c *CacheWrapper) scheduleCheckForRecoveries(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for node := range c.failedNodes {
				c.mu.Lock()
				failed := c.failedNodes[node]
				c.mu.Unlock()
				if failed {
					go c.checkNodeRecovered(node, ctx)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// checkNodeRecovered checks if a given node has recovered.
func (c *CacheWrapper) checkNodeRecovered(node int, ctx context.Context) {
	if _, status := c.sendRequestToNode(node, "GET", "/get", make([]byte, 0)); status != http.StatusServiceUnavailable {
		go markRecoveryDetection(time.Now(), node)
		c.mu.Lock()
		c.failedNodes[node] = false
		c.numFailDetections[node] = 0
		c.mu.Unlock()
	}
}

func (c *CacheWrapper) addNode(address string, maxMemMbs int, maxMemoryPolicy string, nodeId int, updateInterval float64) int {

	type kv struct {
		Id              int     `json:"id"`
		MaxMemMbs       int     `json:"maxMemMbs"`
		MaxMemoryPolicy string  `json:"maxMemoryPolicy"`
		UpdateInterval  float64 `json:"updateInterval"`
	}
	var jsonPayload = kv{
		Id:              nodeId,
		MaxMemMbs:       maxMemMbs,
		MaxMemoryPolicy: maxMemoryPolicy,
		UpdateInterval:  updateInterval,
	}

	jsonPayloadBytes, err := json.Marshal(jsonPayload)
	if err != nil {
		panic(err)
	}

	// This is a GET request, so no payload is sent
	response, status := sendRequest2("GET", fmt.Sprintf("%s/newNode", address), []byte(jsonPayloadBytes))
	if status != http.StatusOK {
		log.Printf("Received response from %s: %v: %s\n", fmt.Sprintf("%s/newNode", address), status, response)
	} else {
		log.Printf("Node %d created successfully\n", nodeId)
	}

	c.nodes[nodeId] = address
	return nodeId
}

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {

	start := time.Now()

	nodes := c.GetNodes(key)
	primaryNodeId := nodes[0]
	currentNodeId := primaryNodeId

	for i := 1; i < len(nodes); i++ {
		if !c.isNodeFailed(currentNodeId) {
			go cacheMeasure(start, key, currentNodeId, "READ", nil, 0, false)
			return c.sendGet(key, currentNodeId, start, currentNodeId == primaryNodeId)
		}
	}

	go cacheMeasure(start, key, primaryNodeId, "READ", errors.New("All nodes failed"), 0, false)
	return nil, errors.New("All nodes failed"), 0
}

func (c *CacheWrapper) sendGet(key string, nodeId int, start time.Time, getBackup bool) (map[string][]byte, error, int64) {
	type kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
	}
	var jsonPayload = kv{
		Key:    key,
		Fields: make([]string, 0),
	}

	jsonPayloadBytes, err := json.Marshal(jsonPayload)
	if err != nil {
		panic(err)
	}
	var response string
	var status int

	// This is a GET request, so no payload is sent
	if getBackup {
		response, status = c.sendRequestToNode(nodeId, "GET", "getBackup", []byte(jsonPayloadBytes))
	} else {
		response, status = c.sendRequestToNode(nodeId, "GET", "get", []byte(jsonPayloadBytes))
	}
	if status != http.StatusOK {
		err = errors.New(response)
		if response != "redis: nil\n" {
			if c.markFailureDetection(nodeId) {
				markFailureDetection(start, key, nodeId, "GET")
			}
			err = redis.Nil
		}
		go cacheMeasure(start, key, nodeId, "READ", err, 0, false)
		return nil, err, 0
	}
	// Define a structure to unmarshal the JSON string
	var data struct {
		Value map[string]string `json:"value"`
		Size  int               `json:"size"`
	}

	// Unmarshal the JSON string
	if err = json.Unmarshal([]byte(response), &data); err != nil {
		log.Fatal(err)
	}

	// Convert the string values in the map to []byte
	result := make(map[string]map[string][]byte)
	result["value"] = make(map[string][]byte)
	for key, valueStr := range data.Value {
		decodedBytes, err := base64.StdEncoding.DecodeString(valueStr)
		if err != nil {
			log.Fatal(err)
		}
		result["value"][key] = decodedBytes
	}
	go cacheMeasure(start, key, nodeId, "READ", err, int64(data.Size), false)
	return result["value"], nil, int64(data.Size)
}

func (c *CacheWrapper) sendSet(nodeId int, key string, value map[string][]byte, backUpNode int, start time.Time, setBackup bool) (error, int64) {
	type kv struct {
		Key        string            `json:"key"`
		Value      map[string][]byte `json:"value"`
		BackUpNode int               `json:"backupNode"`
	}
	var jsonPayload = kv{
		Key:        key,
		Value:      value,
		BackUpNode: backUpNode,
	}

	jsonPayloadBytes, err := json.Marshal(jsonPayload)
	if err != nil {
		panic(err)
	}
	var response string
	var status int

	// This is a GET request, so no payload is sent
	if setBackup {
		response, status = c.sendRequestToNode(nodeId, "POST", "setBackup", []byte(jsonPayloadBytes))
	} else {
		response, status = c.sendRequestToNode(nodeId, "POST", "set", []byte(jsonPayloadBytes))
	}
	if status != http.StatusOK {
		err = errors.New(response)
		go cacheMeasure(start, key, nodeId, "INSERT", err, 0, false)
		return err, 0
	}
	// Define a structure to unmarshal the JSON string
	var data struct {
		Size int `json:"size"`
	}

	// Unmarshal the JSON string
	if err = json.Unmarshal([]byte(response), &data); err != nil {
		log.Fatal(err)
	}
	go cacheMeasure(start, key, nodeId, "INSERT", err, int64(data.Size), false)
	return nil, int64(data.Size)
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {

	start := time.Now()

	nodes := c.GetNodes(key)
	primaryNodeId := nodes[0]
	currentNodeId := primaryNodeId

	for i := 1; i < len(nodes); i++ {
		backupNodeId := nodes[i]
		if !c.isNodeFailed(currentNodeId) {
			go cacheMeasure(start, key, currentNodeId, "INSERT", errors.New("All nodes failed"), 0, false)
			return c.sendSet(primaryNodeId, key, value, backupNodeId, start, true)
		}
		currentNodeId++
	}

	go cacheMeasure(start, key, currentNodeId, "INSERT", errors.New("All nodes failed"), 0, false)
	return errors.New("All nodes failed"), 0
}

func (c *CacheWrapper) markFailureDetection(node int) bool {
	isOverThreshhold := false
	c.mu.Lock()
	if c.failedNodes[node] {
		c.mu.Unlock()
		return false
	}
	c.numFailDetections[node]++
	if c.numFailDetections[node] > c.threshhold {
		c.failedNodes[node] = true
		c.numFailDetections[node] = 0
		isOverThreshhold = true
	}
	c.mu.Unlock()
	return isOverThreshhold
}

func (c *CacheWrapper) isNodeFailed(node int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failedNodes[node]
}

func StringHash(key string) int {
	// Using MurmurHash to compute the hash
	hash := murmur3.New32() // create a new 32-bit MurmurHash3 hash
	if _, err := hash.Write([]byte(key)); err != nil {
		panic(err)
	}
	return int(hash.Sum32())
}

func (c *CacheWrapper) GetNode(key string) int {
	return c.nodeOrders[(StringHash(key) % len(c.nodeOrders))][0]
	//return (util.StringHash(key) % len(c.nodes))
}

func (c *CacheWrapper) GetNodes(key string) []int {
	return c.nodeOrders[(StringHash(key) % len(c.nodeOrders))]
	//numbers := make([]int, 0, len(c.nodes))
	//used := make(map[int]bool)
	//hash := fmt.Sprintf("%d", util.StringHash64(key))
	//hash2 := fmt.Sprintf("%d", util.StringHash64(hash))
	//hash = hash + hash2
	//
	//for i := 0; i < len(hash); i++ {
	//	digit := int(hash[i]-'0') % len(c.nodes)
	//	if !used[digit] {
	//		numbers = append(numbers, digit)
	//		used[digit] = true
	//	}
	//	if len(numbers) == len(c.nodes) {
	//		break
	//	}
	//}
	//
	//// Fill in any missing nodes
	//for i := 0; len(numbers) < len(c.nodes); i++ {
	//	index := i % len(c.nodes)
	//	if !used[index] {
	//		numbers = append(numbers, index)
	//		used[index] = true
	//	}
	//}
	//
	//return numbers
}
