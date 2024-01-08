package bench

import (
	"benchmark/db"
	"benchmark/measurements"
	bconfig "benchmark_config"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Benchmark struct {
	database             *db.DbWrapper         // wrapper for database operations
	NodeConfigs          []*bconfig.Config     // configurations for each cache node
	NumRequests          int                   // total number of requests to be processed
	ReadPercentage       float64               // ratio of read operations to total operations
	MaxDuration          time.Duration         // maximum duration for the benchmarking process
	Failures             []*bconfig.Config     // configurations for simulating node Failures
	maxConcurrency       int                   // maximum number of concurrent operations (unused)
	KeyspacePop          []float64             // weights for each keyspace to simulate "hot" keyspaces
	NumPossibleKeys      int                   // total number of possible keys in the system
	VirtualNodes         int                   // number of virtual nodes for consistent hashing
	Start                time.Time             // store initial Start time
	M                    *measurements.Metrics // metrics store
	NodeRing             *NodeRing             // node ring for hashing
	CacheExpiration      int
	CacheCleanupInterval int
	MetricsPath          string
}

// NewBenchmark loads and returns the benchmark configuration
func NewBenchmark(cr string, metricsPath string) *Benchmark {
	// loading config
	config, err := bconfig.GetConfig_()
	if err != nil {
		fmt.Println("Failed to load config:", err)
		os.Exit(-1)
	}

	// set up nodes configs
	nodesConfig := config.Get("cacheNodes")
	cacheNodes := []*bconfig.Config{
		nodesConfig.Get("1").Get(cr),
		nodesConfig.Get("2").Get(cr),
		nodesConfig.Get("3").Get(cr),
		nodesConfig.Get("4").Get(cr),
	}

	// set up database config
	databaseConfig := config.Get("database").Get(cr)
	keyspace := databaseConfig.Get("keyspace").AsString("")
	tableName := databaseConfig.Get("tableName").AsString("")

	// retrieve other benchmark params
	numRequests := config.Get("numRequests").AsInt(0)
	readPercentage := config.Get("readPercentage").AsFloat(0.99)
	maxDuration := config.Get("maxDuration").AsInt(30)

	// set up failure simulation configs
	failuresConfig := config.Get("failures")
	var failures []*bconfig.Config
	for i := 0; i < len(failuresConfig.Value.(map[string]interface{})); i++ {
		failures = append(failures, failuresConfig.Get(fmt.Sprintf("%d", i)))
	}
	maxConcurrency := config.Get("databaseMaxConcurrency").AsInt(0.0)

	// set up keyspace popularity weights and other parameters
	keyspacePop := config.Get("keyspacePop").AsFloatArray()
	hosts := []string{databaseConfig.Get("ip").AsString("localhost")}
	db, err := db.NewDbWrapper(keyspace, tableName, maxConcurrency, hosts...)
	if err != nil {
		log.Panic(err)
	}
	virtualNodes := config.Get("virtualNodes").AsInt(0)
	numPossibleKeys := config.Get("numPossibleKeys").AsInt(0)

	expiration := config.Get("cacheExpiration").AsInt(5)
	cleanupInterval := config.Get("cacheCleanupInterval").AsInt(5)

	// finally, make the benchmark configuration
	return &Benchmark{
		database:             db,
		NodeConfigs:          cacheNodes,
		NumRequests:          numRequests,
		ReadPercentage:       readPercentage,
		MaxDuration:          time.Duration(maxDuration) * time.Second,
		Failures:             failures,
		maxConcurrency:       maxConcurrency,
		KeyspacePop:          keyspacePop,
		NumPossibleKeys:      numPossibleKeys,
		VirtualNodes:         virtualNodes,
		CacheCleanupInterval: cleanupInterval,
		CacheExpiration:      expiration,
		MetricsPath:          metricsPath,
	}
}

// selectKeySpace selects a keyspace index based on probability weights
func selectKeySpace(keySpaces []float64) int {
	var totalWeight float64
	for _, ks := range keySpaces {
		totalWeight += ks
	}

	rnd := rand.Float64() * totalWeight
	for i := 0; i < len(keySpaces); i++ {
		if rnd < keySpaces[i] {
			return i
		}
		rnd -= keySpaces[i]
	}
	return -1 // should not reach here
}

// getSizes iterates through each cache node to retrieve and record its current cache size.
func (b *Benchmark) StartCacheNodes() {
	for j := 0; j < len(b.NodeConfigs); j++ { // iterate through each cache node

		node := b.NodeConfigs[j]
		ip := node.Get("ip").AsString("")                                                                                              // node's IP address
		port := node.Get("port").AsString("")                                                                                          // node's port
		url := fmt.Sprintf("http://%s:%s/start?expiration=%d&cleanUpInterval=%d", ip, port, b.CacheExpiration, b.CacheCleanupInterval) // used to fetch the size from the node
		reqTime := time.Now()                                                                                                          // store the request time for storing cache size

		// send a GET request to get its cache size
		resp, _ := http.Get(url)
		if resp != nil {
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				if err != nil {
					log.Printf("error closing reader: %s", err)
				}
			}(resp.Body)

			// process the response with the size if it's OK
			if resp.StatusCode == http.StatusOK {
				size, err := io.ReadAll(resp.Body)
				if err == nil {
					sizeInt, err2 := strconv.Atoi(string(size))
					if err2 == nil {
						// add the cache size metric
						b.M.AddCacheSize(reqTime, j, int64(sizeInt))
					}
				}
			}
		}
	}
}

// getSizes iterates through each cache node to retrieve and record its current cache size.
func (b *Benchmark) getSizes() {
	for j := 0; j < len(b.NodeConfigs); j++ { // iterate through each cache node
		func(j int) {
			node := b.NodeConfigs[j]
			ip := node.Get("ip").AsString("")                 // node's IP address
			port := node.Get("port").AsString("")             // node's port
			url := fmt.Sprintf("http://%s:%s/size", ip, port) // used to fetch the size from the node
			reqTime := time.Now()                             // store the request time for storing cache size

			// send a GET request to get its cache size
			resp, _ := http.Get(url)
			if resp != nil {
				defer func(Body io.ReadCloser) {
					err := Body.Close()
					if err != nil {
						log.Printf("error closing reader: %s", err)
					}
				}(resp.Body)

				// process the response with the size if it's OK
				if resp.StatusCode == http.StatusOK {
					size, err := io.ReadAll(resp.Body)
					if err == nil {
						sizeInt, err2 := strconv.Atoi(string(size))
						if err2 == nil {
							// add the cache size metric
							b.M.AddCacheSize(reqTime, j, int64(sizeInt))
						}
					}
				}
			}
		}(j)
	}
}

// GenerateRequests generates and dispatches requests based on the configuration
func (b *Benchmark) GenerateRequests(ctx context.Context) {

	sizeOfEachKeyspace := b.NumPossibleKeys

	// create a new Zipf distribution (for generating keys)
	zip := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(sizeOfEachKeyspace))

	var displayPerSecond = 10 // display progress every 10 seconds
	skip := int(math.Round((float64(b.NumRequests) / b.MaxDuration.Seconds()) / float64(displayPerSecond)))

	fmt.Printf("\n")

	for i := 0; i < b.NumRequests; i++ {
		// display progress at regular intervals
		if i%skip == 0 {
			fmt.Printf("\r%-2d seconds elapsed - %d/%d of requests done.", int(math.Round(float64(time.Since(b.Start).Seconds()))), i+1, b.NumRequests)
			// fetch cache sizes at twice the interval of progress display
			if i%(skip*4) == 0 {
				go b.getSizes()
			}
		}

		// exit the loop if context is done (i.e., timeout or cancel)
		if ctx.Err() != nil {
			return
		}

		//select {
		//case <-ctx.Done(): // ctx is cancelled so stop the goroutine
		//	return
		//}

		// select a keyspace based on the weights and record the request with the metrics object
		keyspace := selectKeySpace(b.KeyspacePop)
		b.M.AddKeyspaceRequest(keyspace, time.Now())
		key := fmt.Sprintf("%d:%d", keyspace, zip.Uint64()) // Form the key with keyspace and zipf value
		value := fmt.Sprintf("value-%d", zip.Uint64())      // Create a value for the key

		// execute the request in a new goroutine
		//if i%10 == 0 {
		//	go executeRequest(b, key, value)
		//	// calculate the sleep duration to spread requests randomly and approximately evenly over the run duration
		//	dif := b.maxDuration.Microseconds() - time.Since(b.start).Microseconds()
		//	interval := float64(dif) / float64(b.numRequests-i)
		//	variance := int(math.Round(interval)) + 1 // random variance for the sleep interval
		//	if variance > 100 {
		//		ms := float64(rand.Intn(variance*2)) - 1
		//		wait := time.Duration(ms) * time.Microsecond
		//		time.Sleep(wait) // sleep for the random duration within the variance
		//	}
		//} else {
		//	go executeRequest(b, key, value)
		//}
		go b.executeRequest(key, value)

		// calculate the sleep duration to spread requests randomly and approximately evenly over the run duration
		dif := b.MaxDuration.Microseconds() - time.Since(b.Start).Microseconds()
		interval := float64(dif) / float64(b.NumRequests-i)
		variance := int(math.Round(interval)) + 1 // random variance for the sleep interval
		if variance > 100 {
			ms := float64(rand.Intn(variance*2)) - 1
			wait := time.Duration(ms) * time.Microsecond
			time.Sleep(wait) // sleep for the random duration within the variance
		}
	}
	fmt.Printf("\nDone.\n")
	return
}

// executeRequest determines the node for the key using the nodeRing.hashFunc and sends the request.
// Also records latency and other metrics for each operation.
func (b *Benchmark) executeRequest(key string, value string) {

	// todo send requests in batches

	// select a cache node based on node ring hash
	nodeId := b.NodeRing.GetNode(key)

	// get node info
	node := b.NodeConfigs[nodeId]
	ip := node.Get("ip").AsString("")
	port := node.Get("port").AsString("")
	cacheNodeURL := fmt.Sprintf("http://%s:%s", ip, port)

	metricStart := time.Now()

	// 99% of the time, perform a read operation
	if rand.Intn(100) < int(b.ReadPercentage*100) {
		if b.getValue(cacheNodeURL, key, nodeId, b.database) {
			b.M.AddLatency(metricStart, time.Since(metricStart))
			b.M.AddRequest(time.Now(), "read", nodeId, true)
			return
		} else {
			b.M.AddRequest(time.Now(), "read", nodeId, false)
		}
		// 1% of the time perform a write operation
	} else {
		if b.setValue(cacheNodeURL, key, value, b.database, true) {
			b.M.AddLatency(metricStart, time.Since(metricStart))
			b.M.AddRequest(time.Now(), "write", nodeId, true)
			return
		} else {
			b.M.AddRequest(time.Now(), "write", nodeId, false)
		}
	}

}

// SimulateNodeFailures simulates node failures according to the predefined failure intervals in the Benchmark configuration.
func (b *Benchmark) SimulateNodeFailures(ctx context.Context) {

	// first make sure all nodes are up
	for i := 0; i < len(b.NodeConfigs); i++ {
		go triggerNodeFailureOrRecovery(b.NodeConfigs[i], false)
	}

	for i := 0; i < len(b.Failures); i++ {
		failureConfig := b.Failures[i]
		nodeId := failureConfig.Get("nodeId").AsInt(0)
		timeToFail := failureConfig.Get("timeToFail").AsFloat(0.0)
		ttf := b.Start.Add(time.Duration(float64(b.MaxDuration.Nanoseconds())*timeToFail) * time.Nanosecond)
		failureDuration := failureConfig.Get("failureDuration").AsFloat(0.0)
		fd := time.Duration(float64(b.MaxDuration.Nanoseconds())*failureDuration) * time.Nanosecond
		go b.simulateNodeFailure(ctx, nodeId, ttf, fd)
	}
}

// simulateNodeFailure handles the simulation of a single node failure and recovery.
func (b *Benchmark) simulateNodeFailure(ctx context.Context, nodeIndex int, timeToFail time.Time, failureDuration time.Duration) {

	timeToRecover := timeToFail.Add(failureDuration)

	failTimer := time.NewTimer(time.Until(timeToFail))
	recoverTimer := time.NewTimer(time.Until(timeToRecover))

	for {
		select {
		case <-ctx.Done(): // ctx is cancelled so stop the goroutine
			return
		case <-failTimer.C:
			b.M.AddNodeFailureInterval(nodeIndex, time.Now(), timeToRecover)
			go triggerNodeFailureOrRecovery(b.NodeConfigs[nodeIndex], true)
			failTimer.Stop() // stop the fail timer if it's no longer needed
		case <-recoverTimer.C:
			go triggerNodeFailureOrRecovery(b.NodeConfigs[nodeIndex], false)
			return // end the function after recovery
		}
	}
}

// triggerNodeFailureOrRecovery sends a request to either fail or recover a node
func triggerNodeFailureOrRecovery(nodeConfig *bconfig.Config, fail bool) {
	// send an HTTP request to a specific cache node to simulate failure/recovery
	ip := nodeConfig.Get("ip").AsString("")
	port := nodeConfig.Get("port").AsString("")
	endpoint := "/recover"
	if fail {
		endpoint = "/fail"
	}
	url := fmt.Sprintf("http://%s:%s%s", ip, port, endpoint)

	_, err := http.Get(url)
	if err != nil {
		// log.Printf("Failed to send %s request to %s: %v", endpoint, url, err)
	} else {
		// log.Printf("Sent %s request to %s", endpoint, url)
	}
}

// getValue sends a GET request to the cache node and handle cache hit/miss logic
func (b *Benchmark) getValue(baseURL string, key string, nodeIndex int, db *db.DbWrapper) bool {

	//requestTime := time.Now()
	//valueFromDB, successful := db.Get(key)
	//b.m.AddDatabaseRequest(requestTime, successful)
	//
	//if successful && setValue(baseURL, key, valueFromDB, db, false, b) {
	//	return true
	//}
	//return false

	url := fmt.Sprintf("%s/get?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		// retrieve value from the database
		requestTime := time.Now()
		_, successful := db.Get(key)
		b.M.AddDatabaseRequest(requestTime, successful)

		if successful {
			return true
		}
		return false
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	// if it was a cache miss
	if resp.StatusCode != http.StatusOK {
		// retrieve value from the database
		requestTime := time.Now()
		valueFromDB, successful := db.Get(key)
		b.M.AddDatabaseRequest(requestTime, successful)

		if successful && b.setValue(baseURL, key, valueFromDB, db, false) {
			return true
		}
	} else {
		// cache hit
		_, err := io.ReadAll(resp.Body)
		if err == nil {
			b.M.AddCacheHit(time.Now(), key, nodeIndex)
			return true
		} else {
			log.Printf("error reading response body: %s", err)
		}
	}
	return false
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
// if writeToDb is true, it also writes the key, value pair to the database
func (b *Benchmark) setValue(baseURL, key, value string, db *db.DbWrapper, writeToDb bool) bool {
	if writeToDb {
		requestTime := time.Now()
		successful := db.Put(key, value)
		b.M.AddDatabaseRequest(requestTime, successful)
		if !successful {
			return false
		}
	}

	url := fmt.Sprintf("%s/set?key=%s&value=%s", baseURL, key, value)
	resp, err := http.PostForm(url, nil)
	if err != nil {
		return true
		//log.Printf("SET error: %s\n", err)
		//return false
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		//return true
	}
	//log.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
	//return false
	return true
}
