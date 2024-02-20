package main

import (
	"flag"
	"net/http"
	"node/main/handlers"
)

var clientPort string
var otherNodePort string

func main() {
	flag.StringVar(&clientPort, "p", "8080", "Port for client communication")
	flag.StringVar(&otherNodePort, "n", "8081", "Port for other node communication")
	flag.StringVar(&handlers.RedisAddress, "ra", "127.0.0.1:9042", "address of redis server")
	flag.Var(&handlers.OtherNodes, "nodes", "comma-separated list of other node urls")

	flag.Parse()

	go serveClients()
	serveOtherNodes()
}

func serveClients() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/newNode", handlers.NewNodeHandler)
	mux.HandleFunc("/get", handlers.HandleGet)
	mux.HandleFunc("/getBackup", handlers.HandleGetBackup)
	mux.HandleFunc("/set", handlers.HandleSet)
	mux.HandleFunc("/setBackup", handlers.HandleSetBackup)
	mux.HandleFunc("/fail", handlers.HandleFail)
	mux.HandleFunc("/recover", handlers.HandleRecover)
	mux.HandleFunc("/ping", handlers.HandlePing)
	mux.HandleFunc("/done", handlers.HandleDone)

	// Start the server with the mux as the handler
	err := http.ListenAndServe(":"+clientPort, mux)
	if err != nil {
		panic(err)
	}
}

func serveOtherNodes() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/updateKey", handlers.HandleUpdateKey)

	// Start the server with the mux as the handler
	err := http.ListenAndServe(":"+otherNodePort, mux)
	if err != nil {
		panic(err)
	}
}
