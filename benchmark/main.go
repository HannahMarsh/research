package main

import (
	bconfig "benchmark_config"
	"context"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
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
}

// metrics we want to collect
var (
	goodput    int64 = 0 // successful operations used for periodically updating throughput
	throughput int64 = 0

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
	goodputGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_goodput",
			Help: "Goodput (successful operations per second).",
		},
	)
	throughputGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "benchmark_throughput",
			Help: "Throughput (total operations per second).",
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
	prometheus.MustRegister(goodputGauge)
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
	promEndpoint := config.Get("prom_endpoint").AsString("metrics") // default endpoint is metrics
	promPort := config.Get("prom_port").AsString("9100")            // default port is 9100
	maxDuration := config.Get("maxDuration").AsInt(30)

	hosts := []string{databaseConfig.Get("ip").AsString("localhost")}
	return config_{database: NewDbWrapper(keyspace, hosts...),
		nodeConfigs:    cacheNodes,
		numRequests:    numRequests,
		readPercentage: readPercentage,
		promEndpoint:   promEndpoint,
		maxDuration:    time.Duration(maxDuration),
		promPort:       promPort}
}

func main() {

	config := getConfigs()

	// Create a context that will be cancelled after `runDuration`
	//ctx, cancel := context.WithTimeout(context.Background(), config.maxDuration)
	//defer cancel()

	ctx, cancel := context.WithCancel(context.Background())

	// start an HTTP server for Prometheus scraping
	log.Printf("Prometheus handler started listening on :%s/%s", config.promPort, config.promEndpoint)
	http.Handle("/"+config.promEndpoint, promhttp.Handler())
	go startPrometheusListener(config)

	// start failure simulation routine
	//go simulateNodeFailures(ctx, config.nodeConfigs, 1*time.Second, 1*time.Second)

	// start throughput updater
	go updateThroughput(ctx)

	// start generating requests
	log.Printf("Starting generating requests...")
	generateRequests(ctx, config, config.maxDuration)
	cancel()

	// Wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()
	time.Sleep(2 * time.Second)
	fmt.Println("Program finished, cleaning up...")
	//queryPrometheusMetric(config.promPort, config.promEndpoint)
	displaySummaryStats(config)

	// Prevent main goroutine from exiting
	select {}
}

func getCounterValue(counter *prometheus.CounterVec, label string) int {
	metric, err := counter.GetMetricWithLabelValues(label)
	if err != nil {
		log.Printf("Failed to get counter metric: %v", err)
		return -1
	}
	var metricModel dto.Metric
	if err := metric.Write(&metricModel); err != nil {
		log.Printf("Error writing metric: %v", err)
		return -1
	}
	return int(math.Round(metricModel.Counter.GetValue()))
}

func displaySummaryStats(config config_) {
	fmt.Printf("\n\nSummary Stats:\n\n")
	successfulReadOps := getCounterValue(readOpsCounter, "success")
	unsuccessfulReadOps := getCounterValue(readOpsCounter, "failure")
	totalReadOps := successfulReadOps + unsuccessfulReadOps
	readPercentage := int(math.Round(100 * float64(successfulReadOps) / float64(totalReadOps)))

	successfulWriteOps := getCounterValue(writeOpsCounter, "success")
	unsuccessfulWriteOps := getCounterValue(writeOpsCounter, "failure")
	totalWriteOps := successfulWriteOps + unsuccessfulWriteOps
	writePercentage := int(math.Round(100 * float64(successfulWriteOps) / float64(totalWriteOps)))

	rs := func(length int) string {
		return strings.Repeat("_", length)
	}

	fmt.Printf(" %s\n", rs(38))
	fmt.Printf("| %-10s | %-10s | %-10s |\n", "Operation", "Completed", "Successful")
	fmt.Printf("|%s|%s|%s|\n", rs(12), rs(12), rs(12))
	fmt.Printf("| %-10s | %-10d | %-10s |\n", "Read", totalReadOps, fmt.Sprintf("%d%%", readPercentage))
	fmt.Printf("| %-10s | %-10d | %-10s |\n", "Write", totalWriteOps, fmt.Sprintf("%d%%", writePercentage))
	totalPercentage := int(math.Round(100 * float64(successfulWriteOps+successfulReadOps) / float64(totalWriteOps+totalReadOps)))

	fmt.Printf("| %-10s | %-10d | %-10s |\n", "Total", totalWriteOps+totalReadOps, fmt.Sprintf("%d%%", totalPercentage))
	fmt.Printf("|%s|%s|%s|\n", rs(12), rs(12), rs(12))

	fmt.Printf("\n %s\n", rs(48))
	fmt.Printf("| %-5s | %-10s | %-12s | %-10s |\n", "Node", "Cache Hits", "Cache Misses", "Total")
	fmt.Printf("|%s|%s|%s|%s|\n", rs(7), rs(12), rs(14), rs(12))
	hitsTotal := 0
	missesTotal := 0
	for i := 1; i <= len(config.nodeConfigs); i++ {
		nodeLabel := fmt.Sprintf("node%d", i) // "node1", "node2", etc.
		hits := getCounterValue(cacheHitsCounter, nodeLabel)
		misses := getCounterValue(cacheMissesCounter, nodeLabel)
		hitsTotal += hits
		missesTotal += misses
		total := hits + misses
		fmt.Printf("| %-5d | %-10d | %-12d | %-10d |\n", i, hits, misses, total)
	}
	fmt.Printf("| %-5s | %-10d | %-12d | %-10d |\n", "Total", hitsTotal, missesTotal, hitsTotal+missesTotal)
	fmt.Printf("|%s|%s|%s|%s|\n", rs(7), rs(12), rs(14), rs(12))
}

func queryPrometheusMetric(promPort string, promEndpoint string) {
	url := fmt.Sprintf("http://localhost:%s/%s", promPort, promEndpoint)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error querying Prometheus: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)
	metricsData, err := io.ReadAll(resp.Body)
	// Open the file in append mode. If it doesn't exist, create it with permissions.
	f, err := os.OpenFile("metrics/result.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)
	if _, err := f.WriteString(string(metricsData)); err != nil {
		log.Fatal(err)
	}
}

func startPrometheusListener(config config_) {
	err := http.ListenAndServe(":"+config.promPort, nil)
	if err != nil {
		panic(err)
	}
}

func generateRequests(ctx context.Context, config config_, runDuration time.Duration) {
	readRatio := int(config.readPercentage * 100)
	wait := time.Duration((runDuration.Seconds() / float64(config.numRequests)) * float64(time.Second))
	zip := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(config.numRequests)/5)
	fmt.Printf("\n")

	for i := 0; i < config.numRequests; i++ {

		fmt.Printf("\r%d/%d of requests done.", i+1, config.numRequests)
		// Check if context is done before generating each request
		if ctx.Err() != nil {
			return // Exit if context is cancelled
		}
		time.Sleep(wait)

		key := fmt.Sprintf("%d", zip.Uint64())
		value := fmt.Sprintf("value-%d", zip.Uint64())

		// select a cache node based on load-balancing hash
		nodeIndex := getNodeHash(key, config)
		node := config.nodeConfigs[nodeIndex]

		// get node's url
		ip := node.Get("ip").AsString("")
		port := node.Get("port").AsString("")
		cacheNodeURL := fmt.Sprintf("http://%s:%s", ip, port)

		nodeId := rand.Intn(len(config.nodeConfigs)) + 1
		for i := 0; i < len(config.nodeConfigs); i++ {
			nodeLabel := fmt.Sprintf("node%d", nodeId) // "node1", "node2", etc.
			nodeIsUp := false
			// decide whether to perform a read or write operation
			if rand.Intn(100) < readRatio {
				// 99% of the time, perform a read operation
				nodeIsUp = getValue(ctx, cacheNodeURL, key, nodeLabel, config.database)
			} else {
				// 1% of the time perform a write operation
				nodeIsUp = setValue(ctx, cacheNodeURL, key, value)
			}
			if nodeIsUp {
				atomic.AddInt64(&throughput, 1)
				break
			} else { // the node has failed, try another one
				// choose another node at random (not the same as the current one)
				nodeId = (((nodeId - 1) + rand.Intn(len(config.nodeConfigs)-1)) % len(config.nodeConfigs)) + 1
			}
		}
	}
	fmt.Printf("\nDone.\n")
	return
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
	var hash int
	for i := 0; i < len(key); i++ {
		hash = (hash*31 + int(key[i])) % len(config.nodeConfigs)
	}
	return hash
}

// getValue simulates a read operation by sending a GET request to the cache node and handle cache hit/miss logic
func getValue(ctx context.Context, baseURL, key string, nodeLabel string, db *DbWrapper) bool {
	start := time.Now() // start time for latency measurement

	url := fmt.Sprintf("%s/get?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("GET error: %s\n", err)
		readOpsCounter.WithLabelValues("failure").Inc()
		return false
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		// read the response body to determine if it was a hit or miss
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response body: %s", err)
			readOpsCounter.WithLabelValues("failure").Inc()
			return false
		}
		bodyString := string(bodyBytes)

		// todo how do we know if we get a miss?
		// for example, if the body contains "null" or "not found", consider it a miss
		if bodyString == "" || bodyString == "null" || bodyString == "not found" { // todo replace this later
			log.Printf("Cache miss for key: %s\n", key)
			cacheMissesCounter.WithLabelValues(nodeLabel).Inc()

			// retrieve value from the database
			valueFromDB, keyExists := db.Get(key)

			if keyExists {
				// write the value to the cache
				setValue(ctx, baseURL, key, valueFromDB)
			} else {
				setValue(ctx, baseURL, key, "null") // overwrite value
			}
		} else {
			//log.Printf("Cache hit for key: %s\n", key)
			cacheHitsCounter.WithLabelValues(nodeLabel).Inc()
		}
		//log.Printf("GET successful for key: %s\n", key)
		readOpsCounter.WithLabelValues("success").Inc()
		// update successful operations
		atomic.AddInt64(&goodput, 1)
	} else {
		//log.Printf("GET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		readOpsCounter.WithLabelValues("failure").Inc()
	}
	elapsed := time.Since(start).Seconds()                      // calculate elapsed time in seconds
	opLatencyHistogram.WithLabelValues("read").Observe(elapsed) // record the latency
	return true
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
func setValue(ctx context.Context, baseURL, key, value string) bool {
	start := time.Now() // start time for latency measurement

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

	// update metrics

	elapsed := time.Since(start).Seconds()                       // calculate elapsed time in seconds
	opLatencyHistogram.WithLabelValues("write").Observe(elapsed) // record the latency

	if resp.StatusCode == http.StatusOK {
		//log.Printf("SET successful for key: %s\n", key)
		writeOpsCounter.WithLabelValues("success").Inc()
		// update successful operations
		atomic.AddInt64(&goodput, 1)
		return true
	} else {
		//log.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		writeOpsCounter.WithLabelValues("failure").Inc()
		return false
	}
}

// updateThroughput periodically updates the throughput gauge
func updateThroughput(ctx context.Context) {
	msInterval := 100
	multiplier := 1000.0 / float64(msInterval) // used to convert operations per msInterval to operations per second
	ticker := time.NewTicker(time.Duration(msInterval) * time.Millisecond)
	defer ticker.Stop()
	var prevGoodput int64
	var prevThroughput int64
	for range ticker.C {
		select {
		case <-ctx.Done():
			return // Exit if context is cancelled
		case <-ticker.C:
			// calculate goodput
			curGoodput := atomic.LoadInt64(&goodput)
			curThroughput := atomic.LoadInt64(&throughput)
			goodputPer100Ms := curGoodput - prevGoodput            // operations per msInterval
			throughputPer100Ms := curThroughput - prevThroughput   // operations per msInterval
			goodput := float64(goodputPer100Ms) * multiplier       // operations per second
			throughput := float64(throughputPer100Ms) * multiplier // operations per second
			goodputGauge.Set(goodput)
			throughputGauge.Set(throughput)

			// update previous operation count
			prevGoodput = curGoodput
			prevThroughput = curThroughput
		}
	}
}
