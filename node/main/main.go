package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

var config *Config

func main() {
	var id int
	flag.IntVar(&id, "nodeId", -1, "Id of this node.")
	if flag.Parse(); id == -1 {
		log.Fatalf("Please provide nodeId integer as flag: -nodeId <nodeId>")
	}

	config = GetConfig(id)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done() // Decrement the counter when the goroutine completes
		serveClients()
	}()

	go func() {
		defer wg.Done() // Decrement the counter when the goroutine completes
		serveOtherNodes()
	}()

	// Wait for all goroutines to complete
	wg.Wait()
}

func serveClients() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/newNode", NewNodeHandler)
	mux.HandleFunc("/get", HandleGet)
	mux.HandleFunc("/getBackup", HandleGetBackup)
	mux.HandleFunc("/set", HandleSet)
	mux.HandleFunc("/setBackup", HandleSetBackup)
	mux.HandleFunc("/fail", HandleFail)
	mux.HandleFunc("/recover", HandleRecover)
	mux.HandleFunc("/ping", HandlePing)
	mux.HandleFunc("/done", HandleDone)

	// Start the server with the mux as the handler
	log.Printf("Starting server on port %s\n", config.ClientPort)
	err := http.ListenAndServe(config.ClientPort, mux)
	if err != nil {
		panic(err)
	}
}

func serveOtherNodes() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/updateKey", HandleUpdateKey)

	// Start the server with the mux as the handler
	log.Printf("Starting server on port %s\n", config.NodePort)
	err := http.ListenAndServe(config.NodePort, mux)
	if err != nil {
		panic(err)
	}
}

type Config struct {
	Id         int               `json:"id"`
	Redis      string            `json:"redis"`
	ClientPort string            `json:"clientPort"`
	NodePort   string            `json:"nodePort"`
	AllNodes   map[string]string `json:"allNodes"`
}

func GetConfig(nodeId int) *Config {
	var config Config
	fileName := fmt.Sprintf("node_configs/config%d.json", nodeId)
	// Read the JSON file
	if file, err := os.Open(fileName); err != nil {
		log.Fatalf("Failed to open %s: %v", fileName, err)
	} else {
		defer func(file *os.File) {
			if err = file.Close(); err != nil {
				log.Fatalf("Failed to close config file: %v", err)
			}
		}(file)
		if data, err := io.ReadAll(file); err != nil {
			log.Fatalf("Failed to read config file: %s", err)
		} else if err = json.Unmarshal(data, &config); err != nil {
			log.Fatalf("Error unmarshaling JSON: %v", err)
		}
	}
	return &config
}
