package main

import (
	bconfig "benchmark_config"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type benchmark struct {
	database             *DbWrapper        // wrapper for database operations
	nodeConfigs          []*bconfig.Config // configurations for each cache node
	numRequests          int               // total number of requests to be processed
	readPercentage       float64           // ratio of read operations to total operations
	maxDuration          time.Duration     // maximum duration for the benchmarking process
	failures             []*bconfig.Config // configurations for simulating node failures
	maxConcurrency       int               // maximum number of concurrent operations (unused)
	keyspacePop          []float64         // weights for each keyspace to simulate "hot" keyspaces
	numPossibleKeys      int               // total number of possible keys in the system
	virtualNodes         int               // number of virtual nodes for consistent hashing
	start                time.Time         // store initial start time
	m                    *Metrics          // metrics store
	nodeRing             *NodeRing         // node ring for hashing
	cacheExpiration      int
	cacheCleanupInterval int
}

// getFlags parses command line flags and returns boolean flags
func getFlags() (bool, string) {
	var local bool
	var help bool
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.BoolVar(&local, "l", false, "use local ip addresses for cache nodes_config")

	flag.Parse()

	if help == true {
		fmt.Println("Usage: <program> [-help] [-l]")
		flag.PrintDefaults()
		os.Exit(1)
	}
	var cr = "remote"
	if local {
		cr = "local"
	}
	return local, cr
}

// makeBenchmark loads and returns the benchmark configuration
func makeBenchmark() benchmark {
	// loading config
	config, err := bconfig.GetConfig_()
	if err != nil {
		fmt.Println("Failed to load config:", err)
		os.Exit(-1)
	}
	_, cr := getFlags()

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
	db, err := NewDbWrapper(keyspace, tableName, maxConcurrency, hosts...)
	if err != nil {
		log.Panic(err)
	}
	virtualNodes := config.Get("virtualNodes").AsInt(0)
	numPossibleKeys := config.Get("numPossibleKeys").AsInt(0)

	expiration := config.Get("cacheExpiration").AsInt(5)
	cleanupInterval := config.Get("cacheCleanupInterval").AsInt(5)

	// finally, make the benchmark configuration
	return benchmark{
		database:             db,
		nodeConfigs:          cacheNodes,
		numRequests:          numRequests,
		readPercentage:       readPercentage,
		maxDuration:          time.Duration(maxDuration) * time.Second,
		failures:             failures,
		maxConcurrency:       maxConcurrency,
		keyspacePop:          keyspacePop,
		numPossibleKeys:      numPossibleKeys,
		virtualNodes:         virtualNodes,
		cacheCleanupInterval: cleanupInterval,
		cacheExpiration:      expiration,
	}
}

// main is the entry point for the program
func main() {

	var wg sync.WaitGroup

	// set up the benchmark
	config := makeBenchmark()

	// initialize node ring for consistent hashing
	config.nodeRing = NewNodeRing(len(config.nodeConfigs), config.virtualNodes)

	// set the start time
	config.start = time.Now()

	// initialize metrics
	config.m = NewMetrics(config.start, config.start.Add(config.maxDuration), config)

	// make new plotter
	p := NewPlotter(config.m)

	// context creation for managing the lifecycle of the benchmark
	//ctx, cancel := context.WithCancel(context.Background())

	// context will be cancelled after maxDuration
	ctx, cancel := context.WithTimeout(context.Background(), config.maxDuration)
	defer cancel()

	// start failure simulation routine
	go simulateNodeFailures(config, ctx)

	startCacheNodes(config)

	// start generating requests
	log.Printf("Starting generating requests...")

	go intermediatePlotter(p)

	generateRequests(ctx, config)
	cancel()

	// wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()

	wg.Wait() // wait for all goroutines to finish

	// todo remove this
	time.Sleep(2 * time.Second)

	// make the plots
	p.MakePlots()

	fmt.Println("Benchmark program finished.")

}

func intermediatePlotter(plt *Plotter_) {
	//// Create a ticker that fires every second
	//ticker := time.NewTicker(1 * time.Second)
	//
	//for {
	//	select {
	//	case <-ticker.C:
	//		plt.MakePlotsFrom(time.Now())
	//	}
	//}
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
func startCacheNodes(b benchmark) {
	for j := 0; j < len(b.nodeConfigs); j++ { // iterate through each cache node

		node := b.nodeConfigs[j]
		ip := node.Get("ip").AsString("")                                                                                              // node's IP address
		port := node.Get("port").AsString("")                                                                                          // node's port
		url := fmt.Sprintf("http://%s:%s/start?expiration=%d&cleanUpInterval=%d", ip, port, b.cacheExpiration, b.cacheCleanupInterval) // used to fetch the size from the node
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
						b.m.AddCacheSize(reqTime, j, int64(sizeInt))
					}
				}
			}
		}
	}
}

// getSizes iterates through each cache node to retrieve and record its current cache size.
func getSizes(b benchmark) {
	for j := 0; j < len(b.nodeConfigs); j++ { // iterate through each cache node
		func(j int) {
			node := b.nodeConfigs[j]
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
							b.m.AddCacheSize(reqTime, j, int64(sizeInt))
						}
					}
				}
			}
		}(j)
	}
}

// generateRequests generates and dispatches requests based on the configuration
func generateRequests(ctx context.Context, b benchmark) {

	sizeOfEachKeyspace := b.numPossibleKeys

	// create a new Zipf distribution (for generating keys)
	zip := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(sizeOfEachKeyspace))

	//var displayPerSecond = 10 // display progress every 10 seconds
	//skip := int(math.Round((float64(b.numRequests) / b.maxDuration.Seconds()) / float64(displayPerSecond)))

	fmt.Printf("\n")

	for {
		// display progress at regular intervals
		//if i%skip == 0 {
		//	fmt.Printf("\r%-2d seconds elapsed - %d/%d of requests done.", int(math.Round(float64(time.Since(b.start).Seconds()))), i+1, b.numRequests)
		//	// fetch cache sizes at twice the interval of progress display
		//	if i%(skip*2) == 0 {
		//		go getSizes(b)
		//	}
		//}

		// exit the loop if context is done (i.e., timeout or cancel)
		if ctx.Err() != nil {
			return
		}

		// select a keyspace based on the weights and record the request with the metrics object
		keyspace := selectKeySpace(b.keyspacePop)
		b.m.AddKeyspaceRequest(keyspace, time.Now())
		key := fmt.Sprintf("%d:%d", keyspace, zip.Uint64()) // Form the key with keyspace and zipf value
		value := fmt.Sprintf("value-%d", zip.Uint64())      // Create a value for the key

		// execute the request in a new goroutine
		go executeRequest(b, key, value)

		//// calculate the sleep duration to spread requests randomly and approximately evenly over the run duration
		//dif := b.maxDuration.Microseconds() - time.Since(b.start).Microseconds()
		//interval := float64(dif) / float64(b.numRequests-i)
		//variance := int(math.Round(interval)) + 1 // random variance for the sleep interval
		//if variance > 100 {
		//	ms := float64(rand.Intn(variance*2)) - 1
		//	wait := time.Duration(ms) * time.Microsecond
		//	time.Sleep(wait) // sleep for the random duration within the variance
		//}
	}
	fmt.Printf("\nDone.\n")
	return
}

// executeRequest determines the node for the key using the nodeRing.hashFunc and sends the request.
// Also records latency and other metrics for each operation.
func executeRequest(b benchmark, key string, value string) {

	// select a cache node based on node ring hash
	nodeId := b.nodeRing.GetNode(key)

	// get node info
	node := b.nodeConfigs[nodeId]
	ip := node.Get("ip").AsString("")
	port := node.Get("port").AsString("")
	cacheNodeURL := fmt.Sprintf("http://%s:%s", ip, port)

	metricStart := time.Now()

	// 99% of the time, perform a read operation
	if rand.Intn(100) < int(b.readPercentage*100) {
		if getValue(cacheNodeURL, key, nodeId, b.database, b) {
			b.m.AddLatency(metricStart, time.Since(metricStart))
			b.m.AddRequest(time.Now(), "read", nodeId, true)
			return
		} else {
			b.m.AddRequest(time.Now(), "read", nodeId, false)
		}
		// 1% of the time perform a write operation
	} else {
		if setValue(cacheNodeURL, key, value, b.database, true, b) {
			b.m.AddLatency(metricStart, time.Since(metricStart))
			b.m.AddRequest(time.Now(), "write", nodeId, true)
			return
		} else {
			b.m.AddRequest(time.Now(), "write", nodeId, false)
		}
	}

}

// simulateNodeFailures simulates node failures according to the predefined failure intervals in the benchmark configuration.
func simulateNodeFailures(b benchmark, ctx context.Context) {

	// first make sure all nodes are up
	for i := 0; i < len(b.nodeConfigs); i++ {
		go triggerNodeFailureOrRecovery(b.nodeConfigs[i], false)
	}

	for i := 0; i < len(b.failures); i++ {
		failureConfig := b.failures[i]
		nodeId := failureConfig.Get("nodeId").AsInt(0)
		timeToFail := failureConfig.Get("timeToFail").AsFloat(0.0)
		ttf := b.start.Add(time.Duration(float64(b.maxDuration.Nanoseconds())*timeToFail) * time.Nanosecond)
		failureDuration := failureConfig.Get("failureDuration").AsFloat(0.0)
		fd := time.Duration(float64(b.maxDuration.Nanoseconds())*failureDuration) * time.Nanosecond
		go simulateNodeFailure(b, ctx, nodeId, ttf, fd)
	}
}

// simulateNodeFailure handles the simulation of a single node failure and recovery.
func simulateNodeFailure(b benchmark, ctx context.Context, nodeIndex int, timeToFail time.Time, failureDuration time.Duration) {

	timeToRecover := timeToFail.Add(failureDuration)

	failTimer := time.NewTimer(time.Until(timeToFail))
	recoverTimer := time.NewTimer(time.Until(timeToRecover))

	for {
		select {
		case <-ctx.Done(): // ctx is cancelled so stop the goroutine
			return
		case <-failTimer.C:
			b.m.AddNodeFailureInterval(nodeIndex, time.Now(), timeToRecover)
			go triggerNodeFailureOrRecovery(b.nodeConfigs[nodeIndex], true)
			failTimer.Stop() // stop the fail timer if it's no longer needed
		case <-recoverTimer.C:
			go triggerNodeFailureOrRecovery(b.nodeConfigs[nodeIndex], false)
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
func getValue(baseURL string, key string, nodeIndex int, db *DbWrapper, b benchmark) bool {

	url := fmt.Sprintf("%s/get?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		// retrieve value from the database
		requestTime := time.Now()
		_, successful := db.Get(key)
		b.m.AddDatabaseRequest(requestTime, successful)

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
		b.m.AddDatabaseRequest(requestTime, successful)

		if successful && setValue(baseURL, key, valueFromDB, db, false, b) {
			return true
		}
	} else {
		// cache hit
		_, err := io.ReadAll(resp.Body)
		if err == nil {
			b.m.AddCacheHit(time.Now(), key, nodeIndex)
			return true
		} else {
			log.Printf("error reading response body: %s", err)
		}
	}
	return false
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
// if writeToDb is true, it also writes the key, value pair to the database
func setValue(baseURL, key, value string, db *DbWrapper, writeToDb bool, b benchmark) bool {
	if writeToDb {
		requestTime := time.Now()
		successful := db.Put(key, value)
		b.m.AddDatabaseRequest(requestTime, successful)
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
