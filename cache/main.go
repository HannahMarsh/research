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
	"time"
)

func main() {

	var cacheId string
	var port string
	var help bool
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.StringVar(&port, "port", "1025", "Port to listen on")
	flag.StringVar(&cacheId, "id", "1", "Cache ID (an integer 1 to 5)")

	flag.Parse()

	if help == true {
		fmt.Println("Usage: <program> [-help] -id <cache_id>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Initialize Gocache in-memory store
	geocacheClient := gocache.New(5*time.Minute, 10*time.Minute)
	geocacheStore := gocachestore.NewGoCache(geocacheClient)

	cacheManager := cache.New[string](geocacheStore)

	// HTTP handlers
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
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
			log.Printf("Failed to set key %s with value %s: %v\n", key, value, err)
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
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
		// send the value back to the client
		_, err = fmt.Fprintf(w, "Value for key %s: %s\n", key, value)
		if err != nil {
			log.Printf("Failed to send value: \"%s\" tp client: %v", value, err)
		}
	})

	// Start HTTP server
	log.Println("Cache node running on http://localhost:" + port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
