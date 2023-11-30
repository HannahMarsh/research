package benchmark

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
)

type config_ struct {
	database       *DbWrapper
	nodeConfigs    []*bconfig.Config
	numRequests    int     // total number of requests to send
	readPercentage float64 // percentage of read operations
}

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
	config := bconfig.GetConfig_()
	if config == nil {
		fmt.Println("Failed to load config")
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

	hosts := []string{databaseConfig.Get("ip").AsString("localhost")}
	return config_{database: NewDbWrapper(keyspace, hosts...), nodeConfigs: cacheNodes, numRequests: numRequests, readPercentage: readPercentage}
}

func main() {

	config := getConfigs()

	ctx := context.Background()
	readRatio := int(config.readPercentage * 100)

	// start an HTTP server for Prometheus scraping
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9100", nil); err != nil {
			panic(err)
		}
	}()

	for i := 0; i < config.numRequests; i++ {
		// generate a random key-value pair
		// todo
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", rand.Intn(1000))

		// select a random cache node
		node := config.nodeConfigs[rand.Intn(len(config.nodeConfigs))]
		ip := node.Get("ip").AsString("")
		port := node.Get("port").AsString("")
		cacheNodeURL := fmt.Sprintf("http://%s:%d", ip, port)

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

	// fetch and print metrics
	metricsURL := "http://localhost:9100/metrics"
	resp, err := http.Get(metricsURL)
	if err != nil {
		log.Fatalf("Failed to fetch metrics: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("error closing reader: %s", err)
		}
	}(resp.Body)

	metricsData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read metrics response: %v", err)
	}

	fmt.Println("Metrics Data:")
	fmt.Println(string(metricsData))
}

// getValue simulates a read operation by sending a GET request to the cache node
func getValue(ctx context.Context, baseURL, key string, nodeLabel string, db *DbWrapper) {
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
