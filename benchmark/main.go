package benchmark

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
)

type cacheNode struct {
	ip   string
	port int
}

// List of cache nodes
var cacheNodesRemote = []cacheNode{
	{ip: "132.177.10.81", port: 1025}, // ccl1.cs.unh.edu
	{ip: "132.177.10.82", port: 1025}, // ccl2.cs.unh.edu
	{ip: "132.177.10.83", port: 1025}, // ccl3.cs.unh.edu
	{ip: "132.177.10.84", port: 1025}, // ccl4.cs.unh.edu
}

var cacheNodesLocal = []cacheNode{
	{ip: "localhost", port: 1025}, // ccl1.cs.unh.edu
	{ip: "localhost", port: 1026}, // ccl2.cs.unh.edu
	{ip: "localhost", port: 1027}, // ccl3.cs.unh.edu
	{ip: "localhost", port: 1028}, // ccl4.cs.unh.edu
}

const (
	numRequests    = 1000 // total number of requests to send
	readPercentage = 0.99 // percentage of read operations
)

func main() {
	ctx := context.Background()
	readRatio := int(readPercentage * 100)

	for i := 0; i < numRequests; i++ {
		// Generate a random key-value pair
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", rand.Intn(1000))

		// Select a random cache node
		node := cacheNodesRemote[rand.Intn(len(cacheNodesRemote))]
		cacheNodeURL := fmt.Sprintf("http://%s:%d", node.ip, node.port)

		// Decide whether to perform a read or write operation
		if rand.Intn(100) < readRatio {
			// Perform a read operation
			getValue(ctx, cacheNodeURL, key)
		} else {
			// Perform a write operation
			setValue(ctx, cacheNodeURL, key, value)
		}
	}

	// todo collect and report metrics from Prometheus
}

// getValue simulates a read operation by sending a GET request to the cache node
func getValue(ctx context.Context, baseURL, key string) {
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
		fmt.Printf("GET successful for key: %s\n", key)
	} else {
		fmt.Printf("GET failed for key: %s with status code: %d\n", key, resp.StatusCode)
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
	} else {
		fmt.Printf("SET failed for key: %s with status code: %d\n", key, resp.StatusCode)
	}
}
