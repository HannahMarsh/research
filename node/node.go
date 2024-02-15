//package node
//
//import (
//	"context"
//	"encoding/json"
//	"errors"
//	"fmt"
//	"github.com/go-redis/redis/v8"
//	"log"
//	"math"
//	"sort"
//	"strconv"
//	"strings"
//	"sync"
//	"time"
//)
//
////type Node struct {
////	isFailed        bool
////	failMutex       sync.Mutex
////	redisClient     *redis.Client
////	id              int
////	keyAccessCounts map[string]int64 // Track access counts for keys
////	topKeys         *AtomicByteMap   // Store this node's top hottest keys
////	topKeysLock     sync.Mutex       // Protects topKeys and otherNodesTopKeys
////	isTopKey_       map[string]bool
////	isTopKeyChanged map[string]bool
////	backUpNode      map[string]int
////
////	otherNodes []OtherNode
////}
////
////type AtomicByteMap struct {
////	mu sync.Mutex
////	m  map[string][]byte
////}
////
////func (m *AtomicByteMap) Set(key string, value []byte) {
////	m.mu.Lock()
////	defer m.mu.Unlock()
////	m.m[key] = value
////}
////
////func (m *AtomicByteMap) Get(key string) ([]byte, bool) {
////	m.mu.Lock()
////	defer m.mu.Unlock()
////	val, ok := m.m[key]
////	return val, ok
////}
//
//type AtomicInteMap struct {
//	mu sync.Mutex
//	m  map[string]int
//}
//
//type Cache interface {
//	Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64)
//	Set(ctx context.Context, key string, value map[string][]byte) (error, int64)
//}
//
//type OtherNode interface {
//	ReceiveUpdateFromOtherNode(key string, serializedValue []byte, accessCounts int64)
//}
//
//func NewNode(id int, address string, maxMemMbs int, maxMemoryPolicy string, ctx context.Context, numBackUps int) *Node {
//
//	c := new(Node)
//	c.id = id
//	c.keyAccessCounts = make(map[string]int64)
//	c.topKeys = make(map[string][]byte)
//	c.otherNodesTopKeys = make(map[string][]byte)
//	c.isTopKey_ = make(map[string]bool)
//	c.isTopKeyChanged = make(map[string]bool)
//	c.backUpNode = make(map[string]int)
//
//	opts := &redis.Options{
//		Addr:     address,
//		Password: "", // no password set
//		DB:       0,  // use default DB
//		//PoolSize: 200, // set the pool size to 100
//		//MinIdleConns: 30,                     // maintain at least 10 idle connections
//		//IdleTimeout:  10 * time.Second, // timeout for idle connections
//		//DialTimeout:  10 * time.Second, // timeout for connecting
//		//ReadTimeout:  1 * time.Second,  // timeout for reads
//		//WriteTimeout: 1 * time.Second,  // timeout for writes
//	}
//
//	// Initialize Redis client
//	c.redisClient = redis.NewClient(opts)
//
//	// Set max memory
//	if err := c.redisClient.ConfigSet(ctx, "maxmemory", fmt.Sprintf("%dmb", maxMemMbs)).Err(); err != nil {
//		panic(err)
//	}
//
//	// Verify the configuration
//	maxMemoryRes, err := c.redisClient.ConfigGet(ctx, "maxmemory").Result()
//	if err != nil {
//		panic(err)
//	} // maxMemoryRes is a slice of interfaces, where the actual value is at the second position
//
//	maxMemory, ok := maxMemoryRes[1].(string) // Redis config values are typically strings
//	if !ok {
//		panic("maxmemory is not a string")
//	}
//	expectedMaxMemory := fmt.Sprintf("%d", maxMemMbs*1024*1024) // Convert megabytes to bytes
//	if maxMemory != expectedMaxMemory {
//		panic(fmt.Errorf("maxmemory is not set properly: %s != %s bytes", maxMemory, expectedMaxMemory))
//	}
//
//	// Set max memory policy
//	if err = c.redisClient.ConfigSet(ctx, "maxmemory-policy", maxMemoryPolicy).Err(); err != nil {
//		panic(err)
//	}
//
//	// Verify the configuration
//	maxMemoryPolicyRes, err := c.redisClient.ConfigGet(ctx, "maxmemory-policy").Result()
//	if err != nil {
//		panic(err)
//	} // maxMemoryPolicyRes is a slice of interfaces, where the actual value is at the second position
//	maxMemPolicy, ok := maxMemoryPolicyRes[1].(string)
//	if !ok {
//		panic("maxmemory-policy is not a string")
//	} else if maxMemPolicy != maxMemoryPolicy {
//		panic(fmt.Errorf("maxmemory-policy is not set properly: %s != %s", maxMemPolicy, maxMemoryPolicy))
//	}
//
//	c.isFailed = false
//
//	if err = c.redisClient.FlushDB(ctx).Err(); err != nil {
//		panic(fmt.Errorf("failed to clear cache: %v", err))
//	}
//
//	return c
//}
//
//func (c *Node) SetOtherNodes(otherNodes []*Node) {
//	c.otherNodes = make([]OtherNode, len(otherNodes))
//	for i, node := range otherNodes {
//		c.otherNodes[i] = OtherNode(node)
//	}
//}
//
//func (c *Node) Size(ctx context.Context) int64 {
//	if c.isFailed {
//		return 0
//	}
//	size, err := c.redisClient.DBSize(ctx).Result()
//	if err != nil {
//		//panic(err)
//		return 0
//	}
//	return size
//}
//
//func (c *Node) Recover(ctx context.Context) {
//	c.failMutex.Lock()
//	defer c.failMutex.Unlock()
//	c.isFailed = false
//
//	// clear the cache to simulate an empty state
//	err := c.redisClient.FlushDB(ctx).Err()
//	if err != nil {
//		log.Printf("Failed to clear cache: %v", err)
//	}
//}
//
//func (c *Node) Fail() {
//	c.failMutex.Lock()
//	c.isFailed = true
//	c.failMutex.Unlock()
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	c.keyAccessCounts = make(map[string]int64)
//	c.topKeys = make(map[string][]byte)
//	c.otherNodesTopKeys = make(map[string][]byte)
//	c.isTopKey_ = make(map[string]bool)
//	c.isTopKeyChanged = make(map[string]bool)
//	//for i := 0; i < numBackUps; i++ {
//	//	n.otherNodesTopKeys[i] = make(map[string][]byte)
//	//}
//}
//
//// GetWithTimeout function calls Get and implements a timeout
//func (c *Node) GetWithTimeout(timeout time.Duration, ctx context.Context, key string, fields []string, isBackup bool) (map[string][]byte, error, int64) {
//
//	type getResult struct {
//		result map[string][]byte
//		err    error
//		size   int64
//	}
//
//	// Channel to capture the output from Get
//	getChan := make(chan getResult, 1)
//
//	// Create a context with a timeout
//	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
//	defer cancel()
//
//	go func() {
//		if !isBackup {
//			result, err, size := c.Get(ctxWithTimeout, key, fields)
//			getChan <- getResult{result, err, size}
//		} else {
//			result, err, size := c.GetBackup(ctxWithTimeout, key, fields)
//			getChan <- getResult{result, err, size}
//		}
//
//	}()
//
//	// handle the result or timeout
//	select {
//	case getRes := <-getChan:
//		return getRes.result, getRes.err, getRes.size
//	case <-ctxWithTimeout.Done():
//		return nil, ctxWithTimeout.Err(), 0
//	}
//}
//
//func (c *Node) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {
//	if err, isFailed := c.checkFailed(); isFailed {
//		return nil, err, 0
//	}
//	go c.updateAccessCount(key)
//	size_ := c.Size(ctx)
//
//	str := c.redisClient.Get(ctx, key)
//	val, err := str.Result()
//	if err != nil {
//		return nil, err, size_ // cache miss happens when err == redis.Nil
//	}
//
//	// Deserialize the JSON back into a map
//	var data map[string][]byte
//	err = json.Unmarshal([]byte(val), &data)
//	if err != nil {
//		return nil, err, size_ // Handle JSON deserialization error
//	}
//
//	// If no specific fields are requested, return the full data
//	if len(fields) == 0 {
//		return data, nil, size_
//	}
//
//	// Extract only the requested fields
//	result := make(map[string][]byte)
//	for _, field := range fields {
//		if value, ok := data[field]; ok {
//			result[field] = value
//		}
//	}
//
//	return result, nil, size_
//}
//
//func (c *Node) GetBackup(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {
//	if err, isFailed := c.checkFailed(); isFailed {
//		return nil, err, 0
//	}
//
//	if val, ok := c.GetBackupKV(key); ok {
//
//		size_ := c.Size(ctx)
//
//		// Deserialize the JSON back into a map
//		var data map[string][]byte
//		if err := json.Unmarshal(val, &data); err != nil {
//			return nil, err, size_ // Handle JSON deserialization error
//		}
//
//		// If no specific fields are requested, return the full data
//		if len(fields) == 0 {
//			return data, nil, size_
//		}
//
//		// Extract only the requested fields
//		result := make(map[string][]byte)
//		for _, field := range fields {
//			if value, exists := data[field]; exists {
//				result[field] = value
//			}
//		}
//		return result, nil, size_
//	}
//	// Key is not in the top 1% of the failed node
//	return c.Get(ctx, key, fields)
//}
//
//func (c *Node) Set(ctx context.Context, key string, value map[string][]byte, backupNode int) (error, int64) {
//	if err, isFailed := c.checkFailed(); isFailed {
//		return err, 0
//	}
//	size_ := c.Size(ctx)
//
//	// Serialize the map into a JSON string for storage
//	serializedValue, err := json.Marshal(value)
//	if err != nil {
//		return err, size_ // Handle JSON serialization error
//	}
//
//	c.updateAccessCountAndSetBackup(key, backupNode, serializedValue)
//
//	_, err = c.redisClient.Set(ctx, key, serializedValue, 0).Result() // '0' means no expiration
//	return err, size_
//}
//
//func (c *Node) SetWithTimeout(timeout time.Duration, ctx context.Context, key string, value map[string][]byte, backUpNode int) (error, int64) {
//	// Create a context with a timeout
//	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
//	defer cancel()
//
//	type getResult struct {
//		err  error
//		size int64
//	}
//
//	// Channel to capture the output from Get
//	getChan := make(chan getResult, 1)
//
//	go func() {
//		err, size := c.Set(ctxWithTimeout, key, value, backUpNode)
//		getChan <- getResult{err, size}
//	}()
//
//	// handle the result or timeout
//	select {
//	case getRes := <-getChan:
//		return getRes.err, getRes.size
//	case <-ctxWithTimeout.Done():
//		return ctxWithTimeout.Err(), 0
//	}
//}
//
//func (c *Node) ReceiveUpdateFromOtherNode(key string, serializedValue []byte, accessCounts int64) {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	c.otherNodesTopKeys[key] = serializedValue
//	c.keyAccessCounts[key] = int64(math.Max(float64(c.keyAccessCounts[key]), float64(accessCounts)))
//}
//
//// Method to start the periodic task for updating top keys
//func (c *Node) StartTopKeysUpdateTask(ctx context.Context, updateInterval time.Duration) {
//	ticker := time.NewTicker(updateInterval)
//	go func() {
//		for {
//			select {
//			case <-ticker.C:
//				if !c.IsFailed() {
//					c.recalculateTopKeys()
//				}
//			case <-ctx.Done():
//				ticker.Stop()
//				return
//			}
//		}
//	}()
//}
//
//func (c *Node) SendUpdateToBackUpNode(key string, serializedValue []byte, accessCounts int64) {
//	//for i, node := range n.otherNodes {
//	//	if i != n.id {
//	//		node.ReceiveUpdateFromOtherNode(n.id-1, key, serializedValue, accessCounts)
//	//	}
//	//}
//	if nodeIndex, exists := c.backUpNode[key]; exists {
//		c.otherNodes[nodeIndex].ReceiveUpdateFromOtherNode(key, serializedValue, accessCounts)
//	}
//}
//
//func (c *Node) updateTopKeyValue(key string, serializedValue []byte) {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	c.topKeys[key] = serializedValue
//}
//
//// GetUsedMemory returns the amount of memory used by the Redis instance in bytes.
//func (c *Node) GetUsedMemory(ctx context.Context) (int64, error) {
//	// Execute the INFO command with the 'memory' section.
//	infoCmd := c.redisClient.Info(ctx, "memory")
//	info, err := infoCmd.Result()
//	if err != nil {
//		return 0, err
//	}
//
//	// Parse the used_memory field from the result.
//	for _, line := range strings.Split(info, "\n") {
//		if strings.HasPrefix(line, "used_memory:") {
//			parts := strings.Split(line, ":")
//			if len(parts) == 2 {
//				// Trim any extra whitespace and carriage return characters
//				usedMemoryStr := strings.TrimSpace(parts[1])
//				usedMemoryStr = strings.Trim(usedMemoryStr, "\r")
//
//				usedMemory, err := strconv.ParseInt(usedMemoryStr, 10, 64)
//				if err != nil {
//					return 0, fmt.Errorf("error parsing used_memory: %v", err)
//				}
//				return usedMemory, nil
//			}
//		}
//	}
//
//	return 0, errors.New("used_memory not found in INFO output")
//}
//
//func (c *Node) updateAccessCount(key string) {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	if _, ok := c.keyAccessCounts[key]; !ok {
//		c.keyAccessCounts[key] = 0
//	}
//	c.keyAccessCounts[key]++
//	c.isTopKeyChanged[key] = true
//}
//
//func (c *Node) updateAccessCountAndSetBackup(key string, backupNode int, serializedValue []byte) {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	if _, ok := c.keyAccessCounts[key]; !ok {
//		c.keyAccessCounts[key] = 0
//	}
//	c.keyAccessCounts[key]++
//	c.isTopKeyChanged[key] = true
//	if backupNode != -1 {
//		c.backUpNode[key] = backupNode
//	}
//
//	if _, ok := c.isTopKey_[key]; ok {
//		if c.isTopKey_[key] {
//			c.topKeys[key] = serializedValue
//		}
//	}
//}
//
//func (c *Node) recalculateTopKeys() {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//
//	for key, _ := range c.isTopKeyChanged {
//		if c.isTopKeyChanged[key] {
//			// Get the access count for the specified key
//			if keyAccessCount, ok := c.keyAccessCounts[key]; ok {
//
//				if value, exists := c.topKeys[key]; exists {
//					if value != nil {
//						go c.SendUpdateToBackUpNode(key, value, keyAccessCount)
//					}
//				}
//
//				// Calculate the threshold for the top 1%
//				topOnePercentIndex := len(c.keyAccessCounts) / 100
//
//				// Create a slice to sort keys by access count
//				var sortedKeys []struct {
//					Key   string
//					Count int64
//				}
//				for k, v := range c.keyAccessCounts {
//					sortedKeys = append(sortedKeys, struct {
//						Key   string
//						Count int64
//					}{k, v})
//				}
//				sort.Slice(sortedKeys, func(i, j int) bool {
//					return sortedKeys[i].Count > sortedKeys[j].Count
//				})
//
//				// Check if the key's access count is in the top 1%
//				if topOnePercentIndex > 0 && topOnePercentIndex < len(sortedKeys) {
//					c.isTopKey_[key] = keyAccessCount >= sortedKeys[topOnePercentIndex-1].Count
//					c.isTopKeyChanged[key] = false
//					continue
//				}
//			}
//			c.isTopKey_[key] = false
//			c.isTopKeyChanged[key] = false
//		}
//	}
//}
//
//func (c *Node) IsTopKey(key string) bool {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//
//	if _, ok := c.isTopKey_[key]; ok {
//		return c.isTopKey_[key]
//	}
//	return false
//}
//
//func (c *Node) IsFailed() bool {
//	c.failMutex.Lock()
//	defer c.failMutex.Unlock()
//	return c.isFailed
//}
//
//func (c *Node) checkFailed() (error, bool) {
//	if c.IsFailed() {
//		// time.Sleep(10 * time.Second)
//		return context.DeadlineExceeded, true
//	}
//	return nil, false
//}
//
//func (c *Node) GetBackupKV(key string) ([]byte, bool) {
//	c.topKeysLock.Lock()
//	defer c.topKeysLock.Unlock()
//	if _, ok := c.keyAccessCounts[key]; !ok {
//		c.keyAccessCounts[key] = 0
//	}
//	c.keyAccessCounts[key]++
//	c.isTopKeyChanged[key] = true
//	if val, ok := c.otherNodesTopKeys[key]; ok {
//		return val, true
//	}
//	return nil, false
//}
