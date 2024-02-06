package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"benchmark/util"
	"context"
	"errors"
	"sync"
	"time"
)

// Upon a cache request, the CacheWrapper first determines the set of nodes responsible for the given key using
// consistent hashing:
//	   - It hashes each key to an ordered list of cache nodes. This list dictates the priority in which nodes will be
//	     accessed for all cache operations with that key. For example, an ordering of [3,0,1,2] means that for this
//	     specific key, the client will first attempt a request on node 3. If node 3 has a failure status,
//	     then it will try node 0, and if node 0 has a failed status, it will try nodes 1 and 2, until the request is fulfilled or
//	     all nodes are failed.
//	   - On every  Set operations, the CacheWrapper also identifies the next node in the priority list for that key so that
//	     the primary node can send an update to the identified backup node if the key is ever deemed hot enough in the future.
//
// During a cache request to a node, if the node does not respond within a specified timeout period (cacheTimeout), it
// is considered a failure detection. Consecutive failure detections for each node are counted, and
// if a node accumulates a number of detections above a predefined threshold, the node is marked with a failure status,
// which prevents the CacheWrapper from sending further requests to the node until it is detected as recovered.
//
// Recovery checks are scheduled to run periodically, every second. During one of these checks, the CacheWrapper
// attempts a GetWithTimeout operation for each node marked as failed. If the operation succeeds, or fails with an error
// other than context.DeadlineExceeded, the node is considered recovered and the failure status is removed from that
// node (and its failure detection count is reset to 0).
//
// Backup nodes:
// Each node in the cache cluster maintains a count of how often each key is accessed. Periodically, each node calculates
// its top hottest keys based on these access counts and for each hottest key, it updates the designated backup node for
// that key with their corresponding values. When a node receives this update, it stores this information, which can be used in case the primary node fails.
//
// --------------------------------------------------------------------------------------------------------------------
// Old:

// The CacheWrapper handles a cache request by hashing each key to a primary and a backup node. If the primary node is
// marked with a failure status, the request is redirected to the backup node.

// Each node in the cache cluster tracks the access frequency of each key. Based on these counts, nodes periodically identify
// their top hottest keys and update their designated backup nodes with these keys and their values. This backup data is stored
// by the receiving nodes and used if the primary node fails.

// During Set operations, the CacheWrapper also identifies the backup node so that the primary node can record the backup node's index associated with that key.
// If the key becomes hot, the primary node can update the backup node with the key-value pair every time the hottest keys are periodically recalculated.

// If a node does not respond within the cacheTimeout period during a request, it triggers a failure detection. Each node's
// consecutive failure detections are tracked. Upon exceeding a predefined threshold, a node is marked as failed, suspending
// further requests to it from CacheWrapper until recovery is detected.

// Recovery checks happen every second. In these checks, the CacheWrapper attempts a GetWithTimeout operation on
// each node that has a failure status. If this operation either succeeds or fails with an error other than context.DeadlineExceeded, the node
// is considered recovered, its failed status is cleared, and its failure detection count is reset to 0.

func cacheMeasure(start time.Time, key string, nodeIndex int, operationType string, err error, cacheSize int64, isHottest bool) {
	latency := time.Now().Sub(start)
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

type CacheWrapper struct {
	nodes []*cache.Node
	p     *bconfig.Config
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

func NewCache(p *bconfig.Config, ctx context.Context) *CacheWrapper {
	c := CacheWrapper{}
	c.p = p
	c.ctx = ctx
	//c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value, p.Cache.EnableReconfiguration.Value)
	//c.memNodes = make(map[string][]int)
	c.cacheTimeout = time.Duration(2000) * time.Millisecond
	c.failedNodes = make(map[int]bool)
	c.numFailDetections = make(map[int]int)
	c.threshhold = 1000
	c.nodeOrders = permute(util.CreateArray(len(p.Cache.Nodes)))
	c.nodes = make([]*cache.Node, 0, len(p.Cache.Nodes))

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig, ctx, len(p.Cache.Nodes))
		c.failedNodes[i] = false
	}
	for _, node := range c.nodes {
		node.SetOtherNodes(c.nodes)
	}
	updateInterval := time.Duration((time.Duration(p.Workload.TargetExecutionTime.Value)*time.Second).Milliseconds()/100) * time.Millisecond
	for _, node := range c.nodes {
		node.StartTopKeysUpdateTask(ctx, updateInterval)
	}
	go c.scheduleFailures()
	go c.scheduleCheckForRecoveries(ctx)
	return &c
}

func (c *CacheWrapper) scheduleFailures() {
	for i := 0; i < len(c.p.Cache.Nodes); i++ {
		nodeIndex := i

		warmUpTime := time.Duration(c.p.Measurements.WarmUpTime.Value) * time.Second
		targetRunningTime := float64(c.p.Workload.TargetExecutionTime.Value)
		for j := 0; j < len(c.p.Cache.Nodes[nodeIndex].FailureIntervals); j++ {
			failureIndex := j
			interval := c.p.Cache.Nodes[nodeIndex].FailureIntervals[failureIndex]
			startDelay := (time.Duration(interval.Start*targetRunningTime) * time.Second) + warmUpTime
			endDelay := (time.Duration(interval.End*targetRunningTime) * time.Second) + warmUpTime

			// Schedule node failure
			failTimer := time.AfterFunc(startDelay, func() {
				go c.nodes[nodeIndex].Fail()
				//c.nodeRing.ReconfigureRingAfterFailure(nodeIndex)
				go nodeFailureMeasure(time.Now(), nodeIndex, true)

				// Schedule node recovery
				recoverTimer := time.AfterFunc(endDelay-startDelay, func() {
					go c.nodes[nodeIndex].Recover(c.ctx)
					//c.nodeRing.ReconfigureRingAfterRecovery(nodeIndex)
					nodeFailureMeasure(time.Now(), nodeIndex, false)
				})
				c.timers = append(c.timers, recoverTimer)
			})

			c.timers = append(c.timers, failTimer)
		}
	}
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
	if _, err, _ := c.nodes[node].GetWithTimeout(c.cacheTimeout, ctx, "key", []string{}, false); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		go markRecoveryDetection(time.Now(), node)
		c.mu.Lock()
		c.failedNodes[node] = false
		c.numFailDetections[node] = 0
		c.mu.Unlock()
	}
}

func (c *CacheWrapper) addNode(p bconfig.NodeConfig, ctx context.Context, numBackUps int) {
	node := cache.NewNode(p, ctx, numBackUps)
	c.nodes = append(c.nodes, node)
}

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {

	nodes := c.GetNodes(key)

	var result map[string][]byte = nil
	var err error = nil
	var size int64 = 0
	var nodeId int = nodes[0]

	if !c.p.Cache.EnableReconfiguration.Value {
		nodes = []int{nodes[0]}
	}

	start := time.Now()

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if !c.isNodeFailed(node) {
			result, err, size = c.nodes[node].GetWithTimeout(c.cacheTimeout, ctx, key, fields, i == 0)
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				go cacheMeasure(start, key, nodeId, metrics2.READ, err, size, false)
				go func() {
					if c.markFailureDetection(node) {
						markFailureDetection(start, key, node, metrics2.READ)
					}
				}()
			} else {
				nodeId = node
				break
			}
		}
	}

	go cacheMeasure(start, key, nodeId, metrics2.READ, err, size, c.nodes[nodeId].IsTopKey(key))
	return result, err, size
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {

	nodes := c.GetNodes(key)

	var err error = nil
	var size int64 = 0
	var nodeId int = nodes[0]

	start := time.Now()

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if !c.isNodeFailed(node) {
			err, size = c.nodes[node].SetWithTimeout(c.cacheTimeout, ctx, key, value, nodes[(i+1)%len(nodes)])
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				go func() {
					if c.markFailureDetection(node) {
						markFailureDetection(start, key, node, metrics2.INSERT)
					}
				}()
			} else {
				nodeId = node
				break
			}
		}
	}

	go cacheMeasure(start, key, nodeId, metrics2.INSERT, err, size, c.nodes[nodeId].IsTopKey(key))
	return err, size
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

func (c *CacheWrapper) GetNodes(key string) []int {
	return c.nodeOrders[util.StringHash(key)%len(c.nodeOrders)]
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
