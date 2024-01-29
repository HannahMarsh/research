package cache

import (
	bconfig "benchmark/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	isFailed          bool
	failMutex         sync.Mutex
	redisClient       *redis.Client
	id                int
	keyAccessCounts   map[string]int64          // Track access counts for keys
	topKeys           map[string][]byte         // Store this node's top hottest keys
	otherNodesTopKeys map[int]map[string][]byte // Store other nodes' top hottest keys and their cached values
	topKeysLock       sync.Mutex                // Protects topKeys and otherNodesTopKeys
	isTopKey_         map[string]bool
	isTopKeyChanged   map[string]bool

	otherNodes []OtherNode
}

type Cache interface {
	Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64)
	Set(ctx context.Context, key string, value map[string][]byte) (error, int64)
}

type OtherNode interface {
	ReceiveUpdateFromOtherNode(nodeIndex int, key string, serializedValue []byte, accessCounts int64)
}

func NewNode(p bconfig.NodeConfig, ctx context.Context, numBackUps int) *Node {
	address := p.Address.Value
	maxMemMbs := p.MaxMemoryMbs.Value
	maxMemoryPolicy := p.MaxMemoryPolicy.Value

	c := new(Node)
	c.id = p.NodeId.Value
	c.keyAccessCounts = make(map[string]int64)
	c.topKeys = make(map[string][]byte)
	c.otherNodesTopKeys = make(map[int]map[string][]byte)
	c.isTopKey_ = make(map[string]bool)
	c.isTopKeyChanged = make(map[string]bool)
	for i := 0; i < numBackUps; i++ {
		c.otherNodesTopKeys[i] = make(map[string][]byte)
	}

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

func (n *Node) SetOtherNodes(otherNodes []*Node) {
	n.otherNodes = make([]OtherNode, len(otherNodes))
	for i, node := range otherNodes {
		n.otherNodes[i] = OtherNode(node)
	}
}

func (n *Node) Size(ctx context.Context) int64 {
	if n.isFailed {
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
	defer n.failMutex.Unlock()
	n.isFailed = true
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()
	numBackUps := len(n.otherNodesTopKeys)
	n.keyAccessCounts = make(map[string]int64)
	n.topKeys = make(map[string][]byte)
	n.otherNodesTopKeys = make(map[int]map[string][]byte)
	n.isTopKey_ = make(map[string]bool)
	n.isTopKeyChanged = make(map[string]bool)
	for i := 0; i < numBackUps; i++ {
		n.otherNodesTopKeys[i] = make(map[string][]byte)
	}
}

func (n *Node) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return nil, err, 0
	}
	n.updateAccessCount(key)
	size_ := n.Size(ctx)

	str := n.redisClient.Get(ctx, key)
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

func (n *Node) GetBackup(ctx context.Context, failedNodeID int, key string, fields []string) (map[string][]byte, error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return nil, err, 0
	}

	if n.IsTopKey(key) {

		if val, ok := n.GetBackupKV(failedNodeID, key); ok {
			n.updateAccessCount(key)
			size_ := n.Size(ctx)

			// Deserialize the JSON back into a map
			var data map[string][]byte
			if err := json.Unmarshal(val, &data); err != nil {
				return nil, err, size_ // Handle JSON deserialization error
			}

			// If no specific fields are requested, return the full data
			if len(fields) == 0 {
				return data, nil, size_
			}

			// Extract only the requested fields
			result := make(map[string][]byte)
			for _, field := range fields {
				if value, exists := data[field]; exists {
					result[field] = value
				}
			}
			return result, nil, size_
		}
	}
	// Key is not in the top 1% of the failed node
	return n.Get(ctx, key, fields)
}

//func (n *Node) SetBackup(ctx context.Context, otherNodeId int, key string, value map[string][]byte) (error, int64) {
//	if n.IsTopKey(key) {
//		if err, isFailed := n.checkFailed(); isFailed {
//			return err, 0
//		}
//		n.updateAccessCount(key)
//		size_ := n.Size(ctx)
//
//		serializedValue, err := json.Marshal(value)
//		if err != nil {
//			return err, size_ // Handle JSON serialization error
//		}
//		n.updateTopKeyValue(key, serializedValue)
//		return nil, size_
//	} else {
//		return n.Set(ctx, key, value)
//	}
//}

func (n *Node) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {
	if err, isFailed := n.checkFailed(); isFailed {
		return err, 0
	}
	size_ := n.Size(ctx)

	// Serialize the map into a JSON string for storage
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err, size_ // Handle JSON serialization error
	}

	n.updateAccessCount(key)
	if n.IsTopKey(key) {
		n.updateTopKeyValue(key, serializedValue)
	}

	_, err = n.redisClient.Set(ctx, key, serializedValue, 0).Result() // '0' means no expiration
	return err, size_
}

func (n *Node) ReceiveUpdateFromOtherNode(nodeIndex int, key string, serializedValue []byte, accessCounts int64) {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()
	n.otherNodesTopKeys[nodeIndex][key] = serializedValue
	n.keyAccessCounts[key] = int64(math.Max(float64(n.keyAccessCounts[key]), float64(accessCounts)))
}

// Method to start the periodic task for updating top keys
func (n *Node) StartTopKeysUpdateTask(ctx context.Context, updateInterval time.Duration) {
	ticker := time.NewTicker(updateInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !n.IsFailed() {
					n.recalculateTopKeys()
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (n *Node) SendUpdateToOtherNodes(key string, serializedValue []byte, accessCounts int64) {
	for i, node := range n.otherNodes {
		if i != n.id {
			node.ReceiveUpdateFromOtherNode(n.id-1, key, serializedValue, accessCounts)
		}
	}
}

func (n *Node) updateTopKeyValue(key string, serializedValue []byte) {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()
	n.topKeys[key] = serializedValue
}

// GetUsedMemory returns the amount of memory used by the Redis instance in bytes.
func (n *Node) GetUsedMemory(ctx context.Context) (int64, error) {
	// Execute the INFO command with the 'memory' section.
	infoCmd := n.redisClient.Info(ctx, "memory")
	info, err := infoCmd.Result()
	if err != nil {
		return 0, err
	}

	// Parse the used_memory field from the result.
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				// Trim any extra whitespace and carriage return characters
				usedMemoryStr := strings.TrimSpace(parts[1])
				usedMemoryStr = strings.Trim(usedMemoryStr, "\r")

				usedMemory, err := strconv.ParseInt(usedMemoryStr, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("error parsing used_memory: %v", err)
				}
				return usedMemory, nil
			}
		}
	}

	return 0, errors.New("used_memory not found in INFO output")
}

func (n *Node) updateAccessCount(key string) {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()
	if _, ok := n.keyAccessCounts[key]; !ok {
		n.keyAccessCounts[key] = 0
	}
	n.keyAccessCounts[key]++
	n.isTopKeyChanged[key] = true
}

func (n *Node) recalculateTopKeys() {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()

	for key, _ := range n.isTopKeyChanged {
		if n.isTopKeyChanged[key] {
			// Get the access count for the specified key
			if keyAccessCount, ok := n.keyAccessCounts[key]; ok {

				if value, exists := n.topKeys[key]; exists {
					if value != nil {
						go n.SendUpdateToOtherNodes(key, value, keyAccessCount)
					}
				}

				// Calculate the threshold for the top 1%
				topOnePercentIndex := len(n.keyAccessCounts) / 100

				// Create a slice to sort keys by access count
				var sortedKeys []struct {
					Key   string
					Count int64
				}
				for k, v := range n.keyAccessCounts {
					sortedKeys = append(sortedKeys, struct {
						Key   string
						Count int64
					}{k, v})
				}
				sort.Slice(sortedKeys, func(i, j int) bool {
					return sortedKeys[i].Count > sortedKeys[j].Count
				})

				// Check if the key's access count is in the top 1%
				if topOnePercentIndex > 0 && topOnePercentIndex < len(sortedKeys) {
					n.isTopKey_[key] = keyAccessCount >= sortedKeys[topOnePercentIndex-1].Count
					n.isTopKeyChanged[key] = false
					continue
				}
			}
			n.isTopKey_[key] = false
			n.isTopKeyChanged[key] = false
		}
	}
}

func (n *Node) IsTopKey(key string) bool {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()

	if _, ok := n.isTopKey_[key]; ok {
		return n.isTopKey_[key]
	}
	return false
}

func (n *Node) IsFailed() bool {
	n.failMutex.Lock()
	defer n.failMutex.Unlock()
	return n.isFailed
}

func (n *Node) checkFailed() (error, bool) {
	if n.IsFailed() {
		return errors.New("simulated failure - cache node is not available"), true
	}
	return nil, false
}

func (n *Node) GetBackupKV(nodeId int, key string) ([]byte, bool) {
	n.topKeysLock.Lock()
	defer n.topKeysLock.Unlock()
	if val, ok := n.otherNodesTopKeys[nodeId][key]; ok {
		return val, true
	}
	return nil, false
}
