package main

import (
	bconfig "benchmark_config"
	"context"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"image/color"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
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
	start      time.Time
	m          *Metrics
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
	databaseRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_database_requests",
			Help: "Number of requests to the database.",
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
	nodeStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "benchmark_node_up_status",
			Help: "Indicates whether a node is up (1) or down (0).",
		},
		[]string{"node"},
	)
)

func init_() {
	// register Metrics with Prometheus
	prometheus.MustRegister(readOpsCounter)
	prometheus.MustRegister(writeOpsCounter)
	prometheus.MustRegister(cacheHitsCounter)
	prometheus.MustRegister(cacheMissesCounter)
	prometheus.MustRegister(opLatencyHistogram)
	prometheus.MustRegister(databaseRequestCounter)
	prometheus.MustRegister(throughputGauge)
	prometheus.MustRegister(goodputGauge)
	prometheus.MustRegister(nodeStatus)
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
	return config_{database: NewDbWrapper(keyspace, tableName, maxConcurrency, hosts...),
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
	init_()

	m = NewMetrics(start, start.Add(config.maxDuration), config)
	p := NewPlotter(m)

	// Create a context that will be cancelled after `runDuration`
	//ctx, cancel := context.WithTimeout(context.Background(), config.maxDuration)
	//defer cancel()

	ctx, cancel := context.WithCancel(context.Background())

	// start an HTTP server for Prometheus scraping
	log.Printf("Prometheus handler started listening on :%s/%s", config.promPort, config.promEndpoint)
	http.Handle("/"+config.promEndpoint, promhttp.Handler())
	go startPrometheusListener(config)

	time.Sleep(1 * time.Second)

	// initialize the status of all nodes as "up"
	for i := 0; i < len(config.nodeConfigs); i++ {
		nodeLabel := fmt.Sprintf("node%d", i+1) // "node1", "node2", etc.
		// To set the status of a node as up:
		nodeStatus.WithLabelValues(nodeLabel).Set(1)
	}

	// start failure simulation routine
	go simulateNodeFailures(config, ctx)

	// start throughput updater
	go updateThroughput(ctx)

	// start generating requests
	log.Printf("Starting generating requests...")
	generateRequests(ctx, config)
	cancel()

	// Wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()

	wg.Wait() // Wait for all goroutines to finish

	time.Sleep(2 * time.Second)

	p.PlotDatabaseRequests("requests_per_second.png")
	p.PlotAllRequests("all_requests_per_second.png")
	p.PlotCacheHits("cache_hit_ratio.png")
	p.PlotLatency("latency.png")
	p.TilePlots("tiled.png")

	fmt.Println("Program finished, cleaning up...")
	//queryPrometheusMetric(config.promPort, config.promEndpoint)
	displaySummaryStats(config)

}

func plot_(p *plot.Plot, width float64, height float64, bottomLeftX float64, bottomLeftY float64) {
	rect, _ := plotter.NewPolygon(plotter.XYs{
		{X: bottomLeftX, Y: bottomLeftY},
		{X: bottomLeftX + width, Y: bottomLeftY},
		{X: bottomLeftX + width, Y: bottomLeftY + height},
		{X: bottomLeftX, Y: bottomLeftY + height},
	})
	rect.Color = color.RGBA{R: 255, G: 0, B: 0, A: 255} // Red color

	// Add the rectangle to the plot
	p.Add(rect)

}

type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				Node string `json:"node"`
			} `json:"Metric"`
			Values [][]interface{} `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func queryPrometheus(query string) (*PrometheusResponse, error) {
	url := "http://localhost:9090/api/v1/query_range?" + query
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)
	metricsData, err := io.ReadAll(resp.Body)
	fmt.Printf("%s", metricsData)
	return nil, nil

}

func getCounterValue(counter *prometheus.CounterVec, label string) int {
	metric, err := counter.GetMetricWithLabelValues(label)
	if err != nil {
		log.Printf("Failed to get counter Metric: %v", err)
		return -1
	}
	var metricModel dto.Metric
	if err := metric.Write(&metricModel); err != nil {
		log.Printf("Error writing Metric: %v", err)
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

	databaseWriteOps := getCounterValue(databaseRequestCounter, "write")
	databaseReadOps := getCounterValue(databaseRequestCounter, "read")

	rs := func(length int) string {
		return strings.Repeat("_", length)
	}

	rsd := func(length int) string {
		if length%2 == 0 {
			return strings.Repeat("- ", length/2)
		}
		return strings.Repeat("- ", length/2) + "-"
	}

	fmt.Printf(" %s\n", rs(58))
	fmt.Printf("| %-10s | %-10s | %-10s | %-17s |\n", "Operation", "Completed", "Successful", "Database Requests")
	fmt.Printf("|%s|%s|%s|%s|\n", rs(12), rs(12), rs(12), rs(19))
	fmt.Printf("| %-10s | %-10d | %-10s | %-17d |\n", "Read", totalReadOps, fmt.Sprintf("%d%%", readPercentage), databaseReadOps)
	fmt.Printf("| %-10s | %-10d | %-10s | %-17d |\n", "Write", totalWriteOps, fmt.Sprintf("%d%%", writePercentage), databaseWriteOps)
	totalPercentage := int(math.Round(100 * float64(successfulWriteOps+successfulReadOps) / float64(totalWriteOps+totalReadOps)))
	fmt.Printf("|%s|%s|%s|%s|\n", rsd(12), rsd(12), rsd(12), rsd(19))
	fmt.Printf("| %-10s | %-10d | %-10s | %-17d |\n", "Total", totalWriteOps+totalReadOps, fmt.Sprintf("%d%%", totalPercentage), databaseWriteOps+databaseReadOps)
	fmt.Printf("|%s|%s|%s|%s|\n", rs(12), rs(12), rs(12), rs(19))

	fmt.Printf("\n %s\n", rs(56))
	fmt.Printf("| %-5s | %-10s | %-14s | %-15s |\n", "Node", "Cache Hits", "Total Requests", "Cache Hit ratio")
	fmt.Printf("|%s|%s|%s|%s|\n", rs(7), rs(12), rs(16), rs(17))
	hitsTotal := 0
	missesTotal := 0
	for i := 1; i <= len(config.nodeConfigs); i++ {
		nodeLabel := fmt.Sprintf("node%d", i) // "node1", "node2", etc.
		hits := getCounterValue(cacheHitsCounter, nodeLabel)
		misses := getCounterValue(cacheMissesCounter, nodeLabel)
		hitsTotal += hits
		missesTotal += misses
		total := hits + misses
		ratio := int(math.Round(100 * float64(hits) / float64(total)))
		fmt.Printf("| %-5d | %-10d | %-14d | %-15s |\n", i, hits, total, fmt.Sprintf("%d%%", ratio))
	}
	fmt.Printf("|%s|%s|%s|%s|\n", rsd(7), rsd(12), rsd(16), rsd(17))
	fmt.Printf("| %-5s | %-10d | %-14d | %-15s |\n", "Total", hitsTotal, hitsTotal+missesTotal, fmt.Sprintf("%d%%", int(math.Round(100*float64(hitsTotal)/float64(hitsTotal+missesTotal)))))
	fmt.Printf("|%s|%s|%s|%s|\n", rs(7), rs(12), rs(16), rs(17))
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
	f, err := os.OpenFile("Metrics/result.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

func generateRequests(ctx context.Context, config config_) {

	zip := rand.NewZipf(rand.New(rand.NewSource(42)), 1.07, 2, uint64(config.numRequests)/100)
	fmt.Printf("\n")
	start := time.Now()

	for i := 0; i < config.numRequests; i++ {

		fmt.Printf("\r%-2d seconds elapsed - %d/%d of requests done.", int(math.Round(float64(time.Since(start).Seconds()))), i+1, config.numRequests)
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
			//readOpsCounter.WithLabelValues("success").Inc()
			//atomic.AddInt64(&throughput, 1)

			m.AddLatency(metricStart, time.Since(metricStart))

			return
		} else {
			//readOpsCounter.WithLabelValues("failure").Inc()
		}
	} else {
		m.AddRequest(time.Now(), "write", nodeId)
		// 1% of the time perform a write operation
		if setValue(cacheNodeURL, key, value, config.database, true) {
			//writeOpsCounter.WithLabelValues("success").Inc()
			//atomic.AddInt64(&throughput, 1)

			m.AddLatency(metricStart, time.Since(metricStart))
			return
		} else {
			//writeOpsCounter.WithLabelValues("failure").Inc()
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

	//go simulateNodeFailure(config, ctx, 0, start.Add(config.maxDuration/2), config.maxDuration/3)
	//
	//go simulateNodeFailure(config, ctx, 1, start.Add(config.maxDuration/2).Add(config.maxDuration/12), config.maxDuration/6)
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
	label := nodeConfig.Get("name").AsString("")
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
		status := 1
		if fail {
			status = 0
		}
		nodeStatus.WithLabelValues(label).Set(float64(status))
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
		log.Printf("GET error: %s\n", err)
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
		//cacheMissesCounter.WithLabelValues(nodeLabel).Inc()

		// retrieve value from the database
		m.AddDatabaseRequest(time.Now())
		valueFromDB, keyExists := db.Get(key)
		//databaseRequestCounter.WithLabelValues("read").Inc()

		if !keyExists {
			valueFromDB = "null"
		}
		if setValue(baseURL, key, valueFromDB, db, false) {
			// record the latency
			//opLatencyHistogram.WithLabelValues("read").Observe(float64(time.Since(start).Microseconds()))
			return true
		}
		// else cache hit
	} else if resp.StatusCode == http.StatusOK {
		_, err := io.ReadAll(resp.Body)
		if err == nil {
			m.AddCacheHit(time.Now(), nodeIndex)
			//cacheHitsCounter.WithLabelValues(nodeLabel).Inc()
			//atomic.AddInt64(&goodput, 1)
			//opLatencyHistogram.WithLabelValues("read").Observe(float64(time.Since(start).Microseconds()))
			return true
		} else {
			log.Printf("error reading response body: %s", err)
		}
	} else {
		//log.Printf("%d: %s\n", resp.StatusCode, resp.Status)
	}
	return false
}

// setValue simulates a write operation by sending a POST request to the cache node to write a key-value pair
func setValue(baseURL, key, value string, db *DbWrapper, writeToDb bool) bool {
	//start := time.Now() // start time for latency measurement

	if writeToDb {
		m.AddDatabaseRequest(time.Now())
		db.Put(key, value)
	}

	//databaseRequestCounter.WithLabelValues("write").Inc()

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
		//log.Printf("SET successful for key: %s\n", key)
		// update successful operations
		//atomic.AddInt64(&goodput, 1)

		// record the latency
		//opLatencyHistogram.WithLabelValues("write").Observe(float64(time.Since(start).Microseconds()))
		return true
	}
	//log.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
	return false
}

// updateThroughput periodically updates the throughput gauge
func updateThroughput(ctx context.Context) {
	//msInterval := 100
	//multiplier := 1000.0 / float64(msInterval) // used to convert operations per msInterval to operations per second
	//ticker := time.NewTicker(time.Duration(msInterval) * time.Millisecond)
	//defer ticker.Stop()
	//var prevGoodput int64
	//var prevThroughput int64
	//for range ticker.C {
	//	select {
	//	case <-ctx.Done():
	//		return // Exit if context is cancelled
	//	case <-ticker.C:
	//		// calculate goodput
	//		curGoodput := atomic.LoadInt64(&goodput)
	//		curThroughput := atomic.LoadInt64(&throughput)
	//		goodputPer100Ms := curGoodput - prevGoodput            // operations per msInterval
	//		throughputPer100Ms := curThroughput - prevThroughput   // operations per msInterval
	//		goodput := float64(goodputPer100Ms) * multiplier       // operations per second
	//		throughput := float64(throughputPer100Ms) * multiplier // operations per second
	//		goodputGauge.Set(goodput)
	//		throughputGauge.Set(throughput)
	//
	//		// update previous operation count
	//		prevGoodput = curGoodput
	//		prevThroughput = curThroughput
	//	}
	//}
}
