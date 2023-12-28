package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/eko/gocache/lib/v4/cache"
	gocachestore "github.com/eko/gocache/store/go_cache/v4"
	gocache "github.com/patrickmn/go-cache"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	isFailed     bool
	failMutex    sync.Mutex
	cacheManager *cache.Cache[string]
)

func main() {
	var cacheId string
	var port string
	var help bool

	// optional command-line flags setup
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.StringVar(&port, "port", "1025", "Port to listen on")
	flag.StringVar(&cacheId, "id", "1", "Cache ID (an integer 1 to 5)")
	flag.Parse()

	// Display usage if help flag is set
	if help == true {
		fmt.Println("Usage: <program> [-help] -port <port> -id <cache_id>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Initialize Gocache in-memory store
	geocacheClient := gocache.New(5*time.Second, 10*time.Second)
	geocacheStore := gocachestore.NewGoCache(geocacheClient)

	// create new cache manager
	cacheManager = cache.New[string](geocacheStore)

	// HTTP handlers
	setupHandlers()

	// Start HTTP server
	log.Println("Cache node running on http://localhost:" + port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

// setupHandlers initializes HTTP routes for cache operations and failure simulation
func setupHandlers() {
	http.HandleFunc("/set", set_)
	http.HandleFunc("/get", get_)
	http.HandleFunc("/fail", fail_)
	http.HandleFunc("/recover", recover_)
}

// recover_ simulates cache recovery from failure
func recover_(w http.ResponseWriter, r *http.Request) {
	failMutex.Lock()
	defer failMutex.Unlock()
	isFailed = false

	// clear the cache to simulate an empty state
	err := cacheManager.Clear(context.Background())
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
		return
	}

	_, err = fmt.Fprintln(w, "Cache node has recovered and is now empty")
	if err != nil {
		log.Printf("Failed to respond to recovery request: %v", err)
	}
}

// fail_ simulates a cache node failure
func fail_(w http.ResponseWriter, r *http.Request) {
	failMutex.Lock()
	defer failMutex.Unlock()
	isFailed = true
	_, err := fmt.Fprintln(w, "Cache node is now simulating failure")
	if err != nil {
		log.Printf("Failed to respond to failure request: %v\n", err)
		return
	}
}

// get_ handles the cache get requests
func get_(w http.ResponseWriter, r *http.Request) {
	if simulateFailure(w) {
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}

	value, err := cacheManager.Get(context.Background(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(w, "Value for key %s: %s\n", key, value)
	if err != nil {
		log.Printf("Failed to return value for key %s: %s: %v\n", key, value, err)
		return
	}
}

// set_ handles the cache set requests
func set_(w http.ResponseWriter, r *http.Request) {
	if simulateFailure(w) {
		return
	}

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "Missing key or value", http.StatusBadRequest)
		return
	}

	err := cacheManager.Set(context.Background(), key, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(w, "Set key %s with value %s\n", key, value)
	if err != nil {
		fmt.Printf("Failed to Set key %s with value %s: %v\n", key, value, err)
		return
	}
}

func simulateFailure(w http.ResponseWriter) bool {
	failMutex.Lock()
	defer failMutex.Unlock()

	if isFailed {
		http.Error(w, "Simulated failure - cache node is not available", http.StatusServiceUnavailable)
		return true
	}
	return false
}
