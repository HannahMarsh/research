package client

import (
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"benchmark/util"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// The CacheWrapper handles a cache request by hashing each key to a primary and a backup node. If the primary node is
// marked with a failure status, the request is redirected to the backup node.

// Each node in the cache cluster tracks the access frequency of each key. Based on these counts, nodes periodically identify
// their top hottest keys and update their designated backup nodes with these keys and their values. This backup data is stored
// by the receiving nodes and used if the primary node fails.

// During Set operations, the CacheWrapper also identifies the backup node so that the primary node can record the backup
//node's index associated with that key. If the key becomes hot, the primary node can update the backup node with the key-value
//pair every time the hottest keys are periodically recalculated.

// If a node does not respond within the cacheTimeout period during a request, it triggers a failure detection. Each node's
// consecutive failure detections are tracked. Upon exceeding a predefined threshold, a node is marked as failed, suspending
// further requests to it from CacheWrapper until recovery is detected.

// Recovery checks happen every second. In these checks, the CacheWrapper attempts a GetWithTimeout operation on each node
//that has a failure status. If this operation either succeeds or fails with an error other than context.DeadlineExceeded,
// the node is considered recovered, its failed status is cleared, and its failure detection count is reset to 0.

func cacheMeasure(start time.Time, key string, nodeIndex int, operationType string, err error, cacheSize int64, isHottest bool) {
	latency := time.Since(start)
	if err != nil {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: false,
				metrics2.OPERATION:  operationType,
				metrics2.ERROR:      err.Error(),
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.NODE_INDEX: nodeIndex,
				metrics2.SIZE:       cacheSize,
				metrics2.KEY:        key,
				metrics2.HOTTEST:    isHottest,
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: true,
				metrics2.OPERATION:  operationType,
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.NODE_INDEX: nodeIndex,
				metrics2.SIZE:       cacheSize,
				metrics2.KEY:        key,
				metrics2.HOTTEST:    isHottest,
			})
	}
}

func markFailureDetection(start time.Time, key string, nodeIndex int, operationType string) {
	latency := time.Now().Sub(start)
	metrics2.AddMeasurement(metrics2.CLIENT_FAILURE_DETECTION, start,
		map[string]interface{}{
			metrics2.OPERATION:  operationType,
			metrics2.LATENCY:    latency.Seconds(),
			metrics2.NODE_INDEX: nodeIndex,
			metrics2.KEY:        key,
		})
}

func markRecoveryDetection(start time.Time, nodeIndex int) {
	latency := time.Now().Sub(start)
	metrics2.AddMeasurement(metrics2.CLIENT_RECOVERY_DETECTION, start,
		map[string]interface{}{
			metrics2.LATENCY:    latency.Seconds(),
			metrics2.NODE_INDEX: nodeIndex,
		})
}

func nodeFailureMeasure(t time.Time, nodeIndex int, isStart bool) {
	if isStart {
		metrics2.AddMeasurement(metrics2.NODE_FAILURE, t,
			map[string]interface{}{
				metrics2.INTERVAL:   metrics2.START,
				metrics2.NODE_INDEX: nodeIndex,
			})
	} else {
		metrics2.AddMeasurement(metrics2.NODE_FAILURE, t,
			map[string]interface{}{
				metrics2.INTERVAL:   metrics2.END,
				metrics2.NODE_INDEX: nodeIndex,
			})
	}
}

func (c *CacheWrapper) sendRequestToNode(nodeId int, method, endpoint string, payload []byte) (string, int) {
	return sendRequest(method, fmt.Sprintf("%s/%s", c.nodes[nodeId], endpoint), payload)
}

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second,
				//KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns: 100,
			//IdleConnTimeout:       10 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
		},
		Timeout: 1 * time.Second,
	}
}

func sendRequest(method, url string, payload []byte) (string, int) {
	client := httpClient
	var reader io.Reader = nil
	if payload != nil {
		reader = bytes.NewBuffer(payload)
	}
	req, err := http.NewRequest(method, url, reader)
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
		//fmt.Println("Error sending request:", err)
		//return "", -1
		return err.Error(), -1
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
	if resp.StatusCode == http.StatusBadRequest {
		log.Printf("Attempted to send: %s, Received response from %v: %s\n", url, resp.StatusCode, string(b))
	}

	if resp.StatusCode == 500 && string(b) != "redis: nil\n" && string(b) != "redis: nil" && string(b) != "context deadline exceeded\n" {
		log.Printf("Received response from %s: %v: %s, %s\n", url, resp.StatusCode, resp.Status, string(b))
	}

	return string(b), resp.StatusCode
}

type CacheWrapper struct {
	nodes map[int]string
	p     *bconfig.Config
	//nodeRing     *cache.NodeRing
	timers []*time.Timer // Timers for scheduling node failures
	ctx    context.Context
	//memNodes          map[string][]int
	mu                sync.RWMutex // Mutex to protect memNodes
	cacheTimeout      time.Duration
	failedNodes       map[int]bool
	numFailDetections map[int]int
	threshhold        int
	nodeOrders        [][]int
}

func permute(nums []int) [][]int {
	var result [][]int
	backtrack(&result, nums, 0)
	return result
}

func backtrack(result *[][]int, nums []int, first int) {
	// If all integers are used up
	if first == len(nums) {
		// Make a deep copy of the current nums (since nums will be modified) and add it to the result
		tmp := make([]int, len(nums))
		copy(tmp, nums)
		*result = append(*result, tmp)
	}
	for i := first; i < len(nums); i++ {
		// Place the i-th integer first in the current permutation
		nums[first], nums[i] = nums[i], nums[first]
		// Use next integers to complete the permutations
		backtrack(result, nums, first+1)
		// Backtrack
		nums[first], nums[i] = nums[i], nums[first]
	}
}

//func permute(nums []int) [][]int {
//	var helper func([]int, int)
//	var res [][]int
//
//	helper = func(nums []int, n int) {
//		if n == 1 {
//			tmp := make([]int, len(nums))
//			copy(tmp, nums)
//			res = append(res, tmp)
//		} else {
//			for i := 0; i < n; i++ {
//				helper(nums, n-1)
//				if n%2 == 1 {
//					nums[i], nums[n-1] = nums[n-1], nums[i]
//				} else {
//					nums[0], nums[n-1] = nums[n-1], nums[0]
//				}
//			}
//		}
//	}
//
//	helper(nums, len(nums))
//	return res
//}

func NewCache(p *bconfig.Config, ctx context.Context) *CacheWrapper {
	c := &CacheWrapper{
		p:                 p,
		ctx:               ctx,
		cacheTimeout:      time.Duration(3000) * time.Millisecond,
		failedNodes:       make(map[int]bool),
		numFailDetections: make(map[int]int),
		threshhold:        1200,
		nodeOrders:        permute(util.CreateArray(len(p.Cache.Nodes))),
		nodes:             make(map[int]string),
	}

	updateInterval := time.Duration((time.Duration(p.Workload.TargetExecutionTime.Value)*time.Second).Milliseconds()/10) * time.Millisecond

	for i := range p.Cache.Nodes {
		nodeId := c.addNode(p.Cache.Nodes[i], updateInterval.Seconds())
		c.failedNodes[nodeId] = false
	}

	go c.scheduleFailures()
	go c.scheduleCheckForRecoveries(ctx)
	return c
}

func (c *CacheWrapper) scheduleFailures() {

	for n := range c.failedNodes {
		c.failedNodes[n] = false
	}

	for i := range c.p.Cache.Nodes {
		node := c.p.Cache.Nodes[i]
		nodeId := node.NodeId.Value
		failureIntervals := node.FailureIntervals
		warmUpTime := time.Duration(c.p.Measurements.WarmUpTime.Value) * time.Second
		targetRunningTime := float64(c.p.Workload.TargetExecutionTime.Value)
		for j := range failureIntervals {
			interval := failureIntervals[j]
			startDelay := (time.Duration(interval.Start*targetRunningTime) * time.Second) + warmUpTime
			endDelay := (time.Duration(interval.End*targetRunningTime) * time.Second) + warmUpTime

			// Schedule node failure
			failTimer := time.AfterFunc(startDelay, func() {
				go c.sendRequestToNode(nodeId, "POST", "fail", nil)
				c.markFailed(nodeId)
				go nodeFailureMeasure(time.Now(), nodeId, true)

				// Schedule node recovery
				recoverTimer := time.AfterFunc(endDelay-startDelay, func() {
					go c.sendRequestToNode(nodeId, "POST", "recover", nil)
					c.markRecovered(nodeId)
					//c.nodeRing.ReconfigureRingAfterRecovery(nodeIndex)
					nodeFailureMeasure(time.Now(), nodeId, false)
				})
				c.timers = append(c.timers, recoverTimer)
			})

			c.timers = append(c.timers, failTimer)
		}
	}
}

func (c *CacheWrapper) scheduleCheckForRecoveries(ctx context.Context) {
	//ticker := time.NewTicker(100 * time.Millisecond)
	//defer ticker.Stop()
	//
	//for {
	//	select {
	//	case <-ticker.C:
	//		for node := range c.failedNodes {
	//			c.mu.Lock()
	//			failed := c.failedNodes[node]
	//			c.mu.Unlock()
	//			if failed {
	//				go c.checkNodeRecovered(node, ctx)
	//			}
	//		}
	//	case <-ctx.Done():
	//		return
	//	}
	//}
}

// checkNodeRecovered checks if a given node has recovered.
func (c *CacheWrapper) checkNodeRecovered(node int, ctx context.Context) {
	//if _, status := c.sendRequestToNode(node, "GET", "/ping", nil); status == http.StatusOK {
	//	go markRecoveryDetection(time.Now(), node)
	//	c.mu.Lock()
	//	c.failedNodes[node] = false
	//	c.numFailDetections[node] = 0
	//	c.mu.Unlock()
	//}
}

func (c *CacheWrapper) addNode(p bconfig.NodeConfig, updateInterval float64) int {
	address := p.Address.Value
	maxMemMbs := p.MaxMemoryMbs.Value
	maxMemoryPolicy := p.MaxMemoryPolicy.Value
	nodeId := p.NodeId.Value

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
	response, status := sendRequest("GET", fmt.Sprintf("%s/newNode", address), []byte(jsonPayloadBytes))
	if status != http.StatusOK {
		log.Printf("Received response from %s: %v: %s\n", fmt.Sprintf("%s/newNode", address), status, response)
	} else {
		log.Printf("Node %d created successfully\n", nodeId)
	}

	c.nodes[nodeId] = address
	return nodeId
}

var empty = make([]string, 0)

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {

	start := time.Now()

	if fields == nil {
		fields = empty
	}

	if !c.p.Cache.EnableReconfiguration.Value {
		nodeId := c.GetNode(key)
		return c.sendGet(key, fields, nodeId, start, false)
	}

	nodes := c.GetNodes(key)
	primaryNodeId := nodes[0]

	for node := range nodes {
		if !c.isNodeFailed(nodes[node]) {
			return c.sendGet(key, fields, nodes[node], start, nodes[node] != primaryNodeId)
		}
	}
	//
	//for i := 1; i < len(nodes); i++ {
	//	currentNodeId = nodes[i]
	//	if !c.isNodeFailed(currentNodeId) {
	//		return c.sendGet(key, fields, currentNodeId, start, currentNodeId == primaryNodeId)
	//	}
	//}

	go cacheMeasure(start, key, primaryNodeId, metrics2.READ, errors.New("All nodes failed"), 0, false)
	return nil, errors.New("All nodes failed"), 0
}

func (c *CacheWrapper) sendGet(key string, fields []string, nodeId int, start time.Time, getBackup bool) (map[string][]byte, error, int64) {
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
		if response != "redis: nil\n" && response != "redis: nil" {
			if c.markFailureDetection(nodeId) {
				markFailureDetection(start, key, nodeId, metrics2.INSERT)
			}
			err = redis.Nil
		} else {
			//err = redis.Nil
		}
		go cacheMeasure(start, key, nodeId, metrics2.READ, err, 0, false)
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
	go cacheMeasure(start, key, nodeId, metrics2.READ, err, int64(data.Size), false)
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
	if status != http.StatusCreated {
		err = errors.New(response)
		go cacheMeasure(start, key, nodeId, metrics2.INSERT, err, 0, false)
		return err, 0
	}
	// Define a structure to unmarshal the JSON string
	var data struct {
		Size int `json:"size"`
	}

	// Unmarshal the JSON string
	if err = json.Unmarshal([]byte(response), &data); err != nil {
		panic(err)
	}
	go cacheMeasure(start, key, nodeId, metrics2.INSERT, nil, int64(data.Size), false)
	return nil, int64(data.Size)
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {

	start := time.Now()

	if !c.p.Cache.EnableReconfiguration.Value {
		nodeId := c.GetNode(key)
		return c.sendSet(nodeId, key, value, nodeId, start, false)
	}

	nodes := c.GetNodes(key)
	primaryNodeId := nodes[0]
	backupNodeId := nodes[1]

	if !c.isNodeFailed(primaryNodeId) {
		//go cacheMeasure(start, key, primaryNodeId, metrics2.INSERT, nil, 0, false)
		return c.sendSet(primaryNodeId, key, value, backupNodeId, start, false)
	} else {
		for i := 1; i < len(nodes); i++ {
			backupNodeId = nodes[i]
			if !c.isNodeFailed(backupNodeId) {
				//go cacheMeasure(start, key, backupNodeId, metrics2.INSERT, nil, 0, false)
				return c.sendSet(backupNodeId, key, value, primaryNodeId, start, true)
			}
		}

		go cacheMeasure(start, key, primaryNodeId, metrics2.INSERT, errors.New("All nodes failed"), 0, false)
		return errors.New("All nodes failed"), 0
	}
}

func (c *CacheWrapper) markFailureDetection(node int) bool {
	//isOverThreshhold := false
	//c.mu.Lock()
	//if c.failedNodes[node] {
	//	c.mu.Unlock()
	//	return false
	//}
	//c.numFailDetections[node]++
	//if c.numFailDetections[node] > c.threshhold {
	//	c.failedNodes[node] = true
	//	c.numFailDetections[node] = 0
	//	isOverThreshhold = true
	//}
	//c.mu.Unlock()
	//return isOverThreshhold
	return c.isNodeFailed(node)
}

func (c *CacheWrapper) isNodeFailed(node int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.failedNodes[node]
}

func (c *CacheWrapper) markFailed(node int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failedNodes[node] = true
}

func (c *CacheWrapper) markRecovered(node int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failedNodes[node] = false
}

func (c *CacheWrapper) GetNode(key string) int {
	return c.nodeOrders[(util.StringHash(key) % len(c.nodeOrders))][0]
	//return (util.StringHash(key) % len(c.nodes))
}

func (c *CacheWrapper) GetNodes(key string) []int {
	return c.nodeOrders[(util.StringHash(key) % len(c.nodeOrders))]
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
