package benchmark

import (
	"context"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"math/rand"
	"net/http"
	"os"
)

type cacheNode struct {
	ip   string
	port int
}

type database_ struct {
	db       *DbWrapper
	keyspace string
	hosts    []string
}

var db_ database_

// List of remote cache nodes
var cacheNodesRemote = []cacheNode{
	{ip: "132.177.10.81", port: 1025}, // ccl1.cs.unh.edu
	{ip: "132.177.10.82", port: 1025}, // ccl2.cs.unh.edu
	{ip: "132.177.10.83", port: 1025}, // ccl3.cs.unh.edu
	{ip: "132.177.10.84", port: 1025}, // ccl4.cs.unh.edu
}

// for local testing (enable with -l flag)
var cacheNodesLocal = []cacheNode{
	{ip: "localhost", port: 1025}, // ccl1.cs.unh.edu
	{ip: "localhost", port: 1026}, // ccl2.cs.unh.edu
	{ip: "localhost", port: 1027}, // ccl3.cs.unh.edu
	{ip: "localhost", port: 1028}, // ccl4.cs.unh.edu
}

const ( // todo change this or put it in a config file
	numRequests    = 1000 // total number of requests to send
	readPercentage = 0.99 // percentage of read operations
)

// metrics we want to track
var (
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
)

func init() {
	// register metrics with Prometheus
	prometheus.MustRegister(readOpsCounter)
	prometheus.MustRegister(writeOpsCounter)
	prometheus.MustRegister(cacheHitsCounter)
	prometheus.MustRegister(cacheMissesCounter)
}

func main() {

	var local bool
	var help bool
	var keyspace string
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.BoolVar(&local, "l", false, "use local ip addresses for cache nodes")
	flag.StringVar(&keyspace, "keyspace", "", "create new keyspace as this")

	flag.Parse()

	if help == true {
		fmt.Println("Usage: <program> [-help] [-l] [-keyspace <keyspace>]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var cacheNodes = cacheNodesRemote
	db_ = database_{
		db:       nil,
		keyspace: keyspace,                  // todo replace with actual keyspace
		hosts:    []string{"132.177.10.85"}, // ccl5.cs.unh.edu
	}

	if local == true {
		cacheNodes = cacheNodesLocal
		db_.hosts = []string{"localhost"}
	}

	db_.db = NewDbWrapper(db_.keyspace, db_.hosts...)

	ctx := context.Background()
	readRatio := int(readPercentage * 100)

	// start an HTTP server for Prometheus scraping
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9100", nil); err != nil {
			panic(err)
		}
	}()

	for i := 0; i < numRequests; i++ {
		// generate a random key-value pair
		// todo
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", rand.Intn(1000))

		// select a random cache node
		node := cacheNodes[rand.Intn(len(cacheNodes))]
		cacheNodeURL := fmt.Sprintf("http://%s:%d", node.ip, node.port)

		// decide whether to perform a read or write operation
		if rand.Intn(100) < readRatio {
			// 99% of the time, perform a read operation
			nodeLabel := fmt.Sprintf("node%d", rand.Intn(len(cacheNodes))+1) // "node1", "node2", etc.
			getValue(ctx, cacheNodeURL, key, nodeLabel)
		} else {
			// 1% of the time perform a write operation
			setValue(ctx, cacheNodeURL, key, value)
		}
	}

	// todo collect and report metrics from Prometheus
}

// getValue simulates a read operation by sending a GET request to the cache node
func getValue(ctx context.Context, baseURL, key string, nodeLabel string) {
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
		// Read the response body to determine if it was a hit or miss
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
			valueFromDB, keyExists := db_.db.Get(key)

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
	} else {
		fmt.Printf("GET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		readOpsCounter.WithLabelValues("failure").Inc()
	}
}

// setValue simulates a write operation by sending a POST request to the cache node
func setValue(ctx context.Context, baseURL, key, value string) {
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

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("SET successful for key: %s\n", key)
		writeOpsCounter.WithLabelValues("success").Inc()
	} else {
		fmt.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
		writeOpsCounter.WithLabelValues("failure").Inc()
	}
}
