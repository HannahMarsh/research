package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"benchmark/util"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"time"
)

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
	timers       []*time.Timer // Timers for scheduling node failures
	ctx          context.Context
	memNodes     map[string][]int
	mu           sync.Mutex // Mutex to protect memNodes
	cacheTimeout time.Duration
}

func NewCache(p *bconfig.Config, ctx context.Context) *CacheWrapper {
	c := CacheWrapper{}
	c.p = p
	c.ctx = ctx
	//c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value, p.Cache.EnableReconfiguration.Value)
	c.memNodes = make(map[string][]int)
	c.cacheTimeout = time.Duration(10) * time.Millisecond

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig, ctx, len(p.Cache.Nodes))
	}
	for _, node := range c.nodes {
		node.SetOtherNodes(c.nodes)
	}
	updateInterval := time.Duration((time.Duration(p.Workload.TargetExecutionTime.Value)*time.Second).Milliseconds()/100) * time.Millisecond
	for _, node := range c.nodes {
		node.StartTopKeysUpdateTask(ctx, updateInterval)
	}
	c.scheduleFailures()
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

	start := time.Now()

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		result, err, size = c.nodes[node].GetWithTimeout(c.cacheTimeout, ctx, key, fields)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			nodeId = i
			break
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
		err, size = c.nodes[node].SetWithTimeout(c.cacheTimeout, ctx, key, value)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			nodeId = i
			break
		}
	}

	go cacheMeasure(start, key, nodeId, metrics2.INSERT, err, size, c.nodes[nodeId].IsTopKey(key))
	return err, size
}

func (c *CacheWrapper) GetNodes(key string) []int {

	c.mu.Lock() // Lock before accessing shared resource
	if nodes, exists := c.memNodes[key]; exists {
		c.mu.Unlock() // Unlock as soon as possible
		return nodes
	}
	c.mu.Unlock() // unlock before doing CPU-intensive work

	// create a hash, convert it to an int64, and use it as a seed to initialize the PRNG
	hash := sha1.Sum([]byte(key))
	seed := int64(binary.BigEndian.Uint64(hash[:8]))
	r := rand.New(rand.NewSource(seed))

	// create and shuffle the slice
	numbers := util.CreateArray(len(c.nodes))
	r.Shuffle(len(numbers), func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})

	c.mu.Lock()
	c.memNodes[key] = numbers
	c.mu.Unlock()

	return numbers
}
