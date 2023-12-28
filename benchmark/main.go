package main

import (
	bconfig "benchmark_config"
	"context"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type config_ struct {
	database       *DbWrapper
	nodeConfigs    []*bconfig.Config
	numRequests    int     // total number of requests to send
	readPercentage float64 // percentage of read operations
	prom_port      string
	prom_endpoint  string
}

// metrics we want to track
var (
	successfulOps int64 = 0 // successful operations used for periodically updating throughput

	readOpsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_read_operations_total",
			Help: "Total number of read operations.",
		},
		[]string{"status"},
	)
	writeOpsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_write_operations_total",
			Help: "Total number of write operations.",
		},
		[]string{"status"},
	)
	cacheHitsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_cache_hits_total",
			Help: "Total number of cache hits.",
		},
		[]string{"node"}, // labels for cache nodes
	)
	cacheMissesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_cache_misses_total",
			Help: "Total number of cache misses.",
		},
		[]string{"node"},
	)
	opLatencyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "benchmark_operation_latency_seconds",
			Help:    "Latency of read/write operations.",
			Buckets: prometheus.DefBuckets, // default buckets for now
		},
		[]string{"operation_type"}, // labels for operation type (read or write)
	)
	throughputGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_throughput_operations",
			Help: "Throughput of operations per second.",
		},
	)
)

func init() {
	// register metrics with Prometheus
	prometheus.MustRegister(readOpsCounter)
	prometheus.MustRegister(writeOpsCounter)
	prometheus.MustRegister(cacheHitsCounter)
	prometheus.MustRegister(cacheMissesCounter)
	prometheus.MustRegister(opLatencyHistogram)
	prometheus.MustRegister(throughputGauge)
}

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

	if databaseConfig.Get("create_keyspace").AsBool(false) {
		keyspace = databaseConfig.Get("keyspace").AsString("")
	}
	numRequests := config.Get("numRequests").AsInt(0)
	readPercentage := config.Get("readPercentage").AsFloat(0.99)

	promEndpoint := config.Get("prom_endpoint").AsString("metrics")
	promPort := config.Get("prom_port").AsString("9100")

	hosts := []string{databaseConfig.Get("ip").AsString("localhost")}
	return config_{database: NewDbWrapper(keyspace, hosts...), nodeConfigs: cacheNodes, numRequests: numRequests, readPercentage: readPercentage, prom_endpoint: promEndpoint, prom_port: promPort}
}

func main() {

	config := getConfigs()

	// Define a duration for how long the program should run
	runDuration := 30 * time.Second
	// Create a context that will be cancelled after `runDuration`
	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	// start an HTTP server for Prometheus scraping
	http.Handle("/"+config.prom_endpoint, promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":"+config.prom_port, nil); err != nil {
			panic(err)
		}
	}()

	// start failure simulation routine
	//go simulateNodeFailures(ctx, config.nodeConfigs, 1*time.Second, 1*time.Second)

	// start throughput updater
	go updateThroughput(ctx)

	// start generating requests
	go generateRequests(ctx, config, runDuration)

	// Wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()
	time.Sleep(5 * time.Second)
	fmt.Println("Program finished, cleaning up...")

	//metricsURL := "http://localhost:" + config.prom_port + "/" + config.prom_endpoint

	//Plot(metricsURL)

	// Prevent main goroutine from exiting
	select {}
}

func generateRequests(ctx context.Context, config config_, runDuration time.Duration) {
	readRatio := int(config.readPercentage * 100)
	wait := time.Duration((runDuration.Seconds() / float64(config.numRequests)) * float64(time.Second))
	zipf := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(config.numRequests))
	for i := 0; i < config.numRequests; i++ {

		// Check if context is done before generating each request
		if ctx.Err() != nil {
			return // Exit if context is cancelled
		}
		time.Sleep(wait)

		key := fmt.Sprintf("key-%d", zipf.Uint64())
		value := fmt.Sprintf("value-%d", rand.Intn(1000))

		// select a cache node based on load-balancing hash
		nodeIndex := getNodeHash(key, config)
		node := config.nodeConfigs[nodeIndex]

		// get node's url
		ip := node.Get("ip").AsString("")
		port := node.Get("port").AsString("")
		cacheNodeURL := fmt.Sprintf("http://%s:%s", ip, port)

		// decide whether to perform a read or write operation
		if rand.Intn(100) < readRatio {
			// 99% of the time, perform a read operation
			nodeLabel := fmt.Sprintf("node%d", rand.Intn(len(config.nodeConfigs))+1) // "node1", "node2", etc.
			getValue(ctx, cacheNodeURL, key, nodeLabel, config.database)
		} else {
			// 1% of the time perform a write operation
			setValue(ctx, cacheNodeURL, key, value)
		}
	}
}

// simulateNodeFailures periodically triggers failure and recovery of cache nodes
func simulateNodeFailures(ctx context.Context, nodeConfigs []*bconfig.Config, failDuration, recoverDuration time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return // Exit if context is cancelled
		default:
			for _, nodeConfig := range nodeConfigs {
				// trigger failure
				triggerNodeFailureOrRecovery(nodeConfig, true)
				time.Sleep(failDuration)

				// trigger recovery
				triggerNodeFailureOrRecovery(nodeConfig, false)
				time.Sleep(recoverDuration)
			}
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
		log.Printf("Failed to send %s request to %s: %v", endpoint, url, err)
	} else {
		log.Printf("Sent %s request to %s", endpoint, url)
	}
}

// getNodeHash computes a hash value used for load balancing across cache nodes
func getNodeHash(key string, config config_) int {
	// simple hash function to distribute requests across cache nodes
	// todo ask Aleksey if we need a more complicated hash for selecting which nodes to send requests to
	var hash int
	for i := 0; i < len(key); i++ {
		hash = (hash*31 + int(key[i])) % len(config.nodeConfigs)
	}
	return hash
}

// getValue simulates a read operation by sending a GET request to the cache node and handle cache hit/miss logic
func getValue(ctx context.Context, baseURL, key string, nodeLabel string, db *DbWrapper) {
	start := time.Now() // start time for latency measurement

	url := fmt.Sprintf("%s/get?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("GET error: %s\n", err)
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		// read the response body to determine if it was a hit or miss
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("error reading response body: %s", err)
			return
		}
		bodyString := string(bodyBytes)

		// todo how do we know if we get a miss?
		// for example, if the body contains "null" or "not found", consider it a miss
		if bodyString == "null" || bodyString == "not found" { // todo replace this later
			fmt.Printf("Cache miss for key: %s\n", key)
			cacheMissesCounter.WithLabelValues(nodeLabel).Inc()

			// retrieve value from the database
			valueFromDB, keyExists := db.Get(key)

			if keyExists {
				// write the value to the cache
				setValue(ctx, baseURL, key, valueFromDB)
			} else {
				setValue(ctx, baseURL, key, "null") // todo remove this?
			}
		} else {
			fmt.Printf("Cache hit for key: %s\n", key)
			cacheHitsCounter.WithLabelValues(nodeLabel).Inc()
		}
		fmt.Printf("GET successful for key: %s\n", key)
		readOpsCounter.WithLabelValues("success").Inc()
		// update successful operations
		atomic.AddInt64(&successfulOps, 1)
	} else {
		fmt.Printf("GET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		readOpsCounter.WithLabelValues("failure").Inc()
	}
	elapsed := time.Since(start).Seconds()                      // calculate elapsed time in seconds
	opLatencyHistogram.WithLabelValues("read").Observe(elapsed) // record the latency
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
func setValue(ctx context.Context, baseURL, key, value string) {
	start := time.Now() // start time for latency measurement

	url := fmt.Sprintf("%s/set?key=%s&value=%s", baseURL, key, value)
	resp, err := http.PostForm(url, nil)
	if err != nil {
		fmt.Printf("SET error: %s\n", err)
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	// update metrics

	elapsed := time.Since(start).Seconds()                       // calculate elapsed time in seconds
	opLatencyHistogram.WithLabelValues("write").Observe(elapsed) // record the latency

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("SET successful for key: %s\n", key)
		writeOpsCounter.WithLabelValues("success").Inc()
		// update successful operations
		atomic.AddInt64(&successfulOps, 1)
	} else {
		fmt.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		writeOpsCounter.WithLabelValues("failure").Inc()
	}
}

// updateThroughput periodically updates the throughput gauge
func updateThroughput(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	var prevOps int64
	for range ticker.C {
		select {
		case <-ctx.Done():
			return // Exit if context is cancelled
		case <-ticker.C:
			// calculate throughput
			currentOps := atomic.LoadInt64(&successfulOps)
			opsThisSecond := currentOps - prevOps
			throughput := float64(opsThisSecond) // throughput is operations per second
			throughputGauge.Set(throughput)

			// update previous operation count
			prevOps = currentOps
		}
	}
}
