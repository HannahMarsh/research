package main

import (
	bconfig "benchmark_config"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

type config_ struct {
	database       *DbWrapper
	nodeConfigs    []*bconfig.Config
	numRequests    int           // total number of requests to send
	readPercentage float64       // percentage of read operations
	maxDuration    time.Duration // max duration in seconds
	promPort       string        // prometheus local server port
	promEndpoint   string        // prometheus local endpoint
	failures       []*bconfig.Config
	maxConcurrency int
}

// Metrics we want to collect
var (
	start time.Time
	m     *Metrics
)

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

func getConfigs() config_ {
	config, err := bconfig.GetConfig_()
	if err != nil {
		fmt.Println("Failed to load config:", err)
		os.Exit(-1)
	}
	_, cr := getFlags()

	nodesConfig := config.Get("cacheNodes")
	cacheNodes := []*bconfig.Config{
		nodesConfig.Get("1").Get(cr),
		nodesConfig.Get("2").Get(cr),
		nodesConfig.Get("3").Get(cr),
		nodesConfig.Get("4").Get(cr),
	}

	databaseConfig := config.Get("database").Get(cr)
	var keyspace = ""
	var tableName = ""

	if databaseConfig.Get("create_keyspace").AsBool(false) {
		keyspace = databaseConfig.Get("keyspace").AsString("")
		tableName = databaseConfig.Get("tableName").AsString("")
	}
	numRequests := config.Get("numRequests").AsInt(0)
	readPercentage := config.Get("readPercentage").AsFloat(0.99)
	promEndpoint := config.Get("prom_endpoint").AsString("Metrics") // default endpoint is Metrics
	promPort := config.Get("prom_port").AsString("9100")            // default port is 9100
	maxDuration := config.Get("maxDuration").AsInt(30)

	failuresConfig := config.Get("failures")
	failures := []*bconfig.Config{}
	for i := 0; i < len(failuresConfig.Value.(map[string]interface{})); i++ {
		failures = append(failures, failuresConfig.Get(fmt.Sprintf("%d", i)))
	}
	maxConcurrency := config.Get("databaseMaxConcurrency").AsInt(0.0)

	hosts := []string{databaseConfig.Get("ip").AsString("localhost")}
	db, err := NewDbWrapper(keyspace, tableName, maxConcurrency, hosts...)
	if err != nil {
		log.Panic(err)
	}
	return config_{database: db,
		nodeConfigs:    cacheNodes,
		numRequests:    numRequests,
		readPercentage: readPercentage,
		promEndpoint:   promEndpoint,
		maxDuration:    time.Duration(maxDuration) * time.Second,
		promPort:       promPort,
		failures:       failures,
		maxConcurrency: maxConcurrency,
	}
}

func main() {

	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	start = time.Now()

	config := getConfigs()

	m = NewMetrics(start, start.Add(config.maxDuration), config)
	p := NewPlotter(m)

	// Create a context that will be cancelled after `runDuration`
	//ctx, cancel := context.WithTimeout(context.Background(), config.maxDuration)
	//defer cancel()

	ctx, cancel := context.WithCancel(context.Background())

	// start failure simulation routine
	go simulateNodeFailures(config, ctx)

	// start generating requests
	log.Printf("Starting generating requests...")
	generateRequests(ctx, config)
	cancel()

	// Wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()

	wg.Wait() // Wait for all goroutines to finish

	time.Sleep(2 * time.Second)

	p.MakePlots()

	fmt.Println("Program finished.")

}

func generateRequests(ctx context.Context, config config_) {

	zip := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(config.numRequests)/100)
	fmt.Printf("\n")
	start := time.Now()

	var displayPerSecond = 10
	skip := int(math.Round((float64(config.numRequests) / config.maxDuration.Seconds()) / float64(displayPerSecond)))

	for i := 0; i < config.numRequests; i++ {

		if i%skip == 0 {
			fmt.Printf("\r%-2d seconds elapsed - %d/%d of requests done.", int(math.Round(float64(time.Since(start).Seconds()))), i+1, config.numRequests)
		}

		// Check if context is done before generating each request
		if ctx.Err() != nil {
			return // Exit if context is cancelled
		}

		key := fmt.Sprintf("%d", zip.Uint64())
		value := fmt.Sprintf("value-%d", zip.Uint64())

		go executeRequest(config, key, value)

		dif := config.maxDuration.Microseconds() - time.Since(start).Microseconds()
		interval := float64(dif) / float64(config.numRequests-i)
		variance := int(math.Round(interval)) + 1 // 2x interval in mu
		if variance > 100 {
			ms := float64(rand.Intn(variance*2)) - 1
			wait := time.Duration(ms) * time.Microsecond
			time.Sleep(wait)
		}
	}
	fmt.Printf("\nDone.\n")
	return
}

func executeRequest(config config_, key string, value string) {

	// select a cache node based on load-balancing hash
	nodeId := getNodeHash(key, config)

	// get node info
	node := config.nodeConfigs[nodeId]
	ip := node.Get("ip").AsString("")
	port := node.Get("port").AsString("")
	cacheNodeURL := fmt.Sprintf("http://%s:%s", ip, port)
	nodeLabel := fmt.Sprintf("node%d", nodeId+1) // "node1", "node2", etc.

	metricStart := time.Now()

	// 99% of the time, perform a read operation
	if rand.Intn(100) < int(config.readPercentage*100) {
		m.AddRequest(time.Now(), "read", nodeId)
		if getValue(cacheNodeURL, key, nodeId, nodeLabel, config.database) {
			m.AddLatency(metricStart, time.Since(metricStart))
			return
		}
		// 1% of the time perform a write operation
	} else {
		m.AddRequest(time.Now(), "write", nodeId)
		if setValue(cacheNodeURL, key, value, config.database, true) {
			m.AddLatency(metricStart, time.Since(metricStart))
			return
		}
	}

}

func simulateNodeFailures(config config_, ctx context.Context) {

	// first make sure all nodes are up
	for i := 0; i < len(config.nodeConfigs); i++ {
		go triggerNodeFailureOrRecovery(i, config.nodeConfigs[i], false)
	}

	for i := 0; i < len(config.failures); i++ {
		failureConfig := config.failures[i]
		nodeId := failureConfig.Get("nodeId").AsInt(0)
		timeToFail := failureConfig.Get("timeToFail").AsFloat(0.0)
		ttf := start.Add(time.Duration(float64(config.maxDuration.Nanoseconds())*timeToFail) * time.Nanosecond)
		failureDuration := failureConfig.Get("failureDuration").AsFloat(0.0)
		fd := time.Duration(float64(config.maxDuration.Nanoseconds())*failureDuration) * time.Nanosecond
		go simulateNodeFailure(config, ctx, nodeId, ttf, fd)
	}
}

func simulateNodeFailure(config config_, ctx context.Context, nodeIndex int, timeToFail time.Time, failureDuration time.Duration) {

	timeToRecover := timeToFail.Add(failureDuration)

	failTimer := time.NewTimer(time.Until(timeToFail))
	recoverTimer := time.NewTimer(time.Until(timeToRecover))

	for {
		select {
		case <-ctx.Done(): // Context is cancelled, stop the goroutine
			return
		case <-failTimer.C:
			m.AddNodeFailureInterval(nodeIndex, time.Now(), timeToRecover)
			go triggerNodeFailureOrRecovery(nodeIndex, config.nodeConfigs[nodeIndex], true)
			failTimer.Stop() // Stop the fail timer if it's no longer needed
		case <-recoverTimer.C:
			go triggerNodeFailureOrRecovery(nodeIndex, config.nodeConfigs[nodeIndex], false)
			return // end the function after recovery
		}
	}
}

// triggerNodeFailureOrRecovery sends a request to either fail or recover a node
func triggerNodeFailureOrRecovery(nodeIndex int, nodeConfig *bconfig.Config, fail bool) {
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
		//log.Printf("Failed to send %s request to %s: %v", endpoint, url, err)
	} else {
		//log.Printf("Sent %s request to %s", endpoint, url)
	}
}

// getNodeHash computes a hash value used for load balancing across cache nodes
func getNodeHash(key string, config config_) int {
	// simple hash function to distribute requests across cache nodes
	var hash int
	for i := 0; i < len(key); i++ {
		hash = (hash*31 + int(key[i])) % len(config.nodeConfigs)
	}
	return hash
}

// getValue simulates a read operation by sending a GET request to the cache node and handle cache hit/miss logic
func getValue(baseURL string, key string, nodeIndex int, nodeLabel string, db *DbWrapper) bool {
	//start := time.Now() // start time for latency measurement

	url := fmt.Sprintf("%s/get?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		// log.Printf("GET error: %s\n", err)
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
		m.AddDatabaseRequest(time.Now())
		valueFromDB, keyExists := db.Get(key)

		if !keyExists {
			valueFromDB = "null"
		}
		if setValue(baseURL, key, valueFromDB, db, false) {
			// record the latency
			return true
		}
		// else cache hit
	} else {
		_, err := io.ReadAll(resp.Body)
		if err == nil {
			m.AddCacheHit(time.Now(), nodeIndex)
			return true
		} else {
			log.Printf("error reading response body: %s", err)
		}
	}
	return false
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
// if writeToDb is true, it also writes the key, value pair to the database
func setValue(baseURL, key, value string, db *DbWrapper, writeToDb bool) bool {
	//start := time.Now() // start time for latency measurement

	if writeToDb {
		m.AddDatabaseRequest(time.Now())
		db.Put(key, value)
	}

	url := fmt.Sprintf("%s/set?key=%s&value=%s", baseURL, key, value)
	resp, err := http.PostForm(url, nil)
	if err != nil {
		log.Printf("SET error: %s\n", err)
		return false
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		return true
	}
	//log.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
	return false
}
