package main

import (
	"context"
	"encoding/json"
	"errors"
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

	globalCtx, globalCancel = context.WithCancel(context.Background())

	for {
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
}

func serveClients() {

	select {
	case <-globalCtx.Done():
		return
	default:
	}
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

	server := &http.Server{Addr: config.ClientPort, Handler: mux}
	//server.SetKeepAlivesEnabled(true)

	go func() {
		<-globalCtx.Done() // Wait for context to be cancelled
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server ListenAndServe: %v", err)
	}
}

func serveOtherNodes() {
	select {
	case <-globalCtx.Done():
		return
	default:
	}

	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/updateKey", HandleUpdateKey)

	// Start the server with the mux as the handler
	log.Printf("Starting server on port %s\n", config.NodePort)
	server := &http.Server{Addr: config.NodePort, Handler: mux}
	//server.SetKeepAlivesEnabled(true)

	go func() {
		<-globalCtx.Done() // Wait for context to be cancelled
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server ListenAndServe: %v", err)
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
	var cnfg Config
	fileName := fmt.Sprintf("nodes/node%d/config.json", nodeId)
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
		} else if err = json.Unmarshal(data, &cnfg); err != nil {
			log.Fatalf("Error unmarshaling JSON: %v", err)
		}
	}
	return &cnfg
}
