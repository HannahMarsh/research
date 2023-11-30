package main

import (
	"context"
	"fmt"
	"github.com/eko/gocache/lib/v4/cache"
	gocachestore "github.com/eko/gocache/store/go_cache/v4"
	gocache "github.com/patrickmn/go-cache"
	"log"
	"net/http"
	"time"
)

func main() {
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
		fmt.Fprintf(w, "Set key %s with value %s\n", key, value)
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
		fmt.Fprintf(w, "Value for key %s: %s\n", key, value)
	})

	// Start HTTP server
	log.Println("Cache node running on http://localhost:1025")
	if err := http.ListenAndServe(":1025", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
