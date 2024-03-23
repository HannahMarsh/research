package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"node/main/node"
	"sync"
	"time"
)

var globalNode *node.Node

var globalCtx context.Context
var globalCancel context.CancelFunc

var globalWg sync.WaitGroup

// params
//var RedisAddress string
//var OtherNodes stringSlice

//// stringSlice is a custom type that satisfies the flag.Value interface.
//type stringSlice []string
//
//func (s *stringSlice) String() string {
//	return strings.Join(*s, ", ")
//}
//
//func (s *stringSlice) Set(value string) error {
//	*s = strings.Split(value, ",")
//	return nil
//}

func isDone() bool {
	if globalCancel == nil {
		return true
	}
	select {
	case <-globalCtx.Done():
		return true
	default:
		return false
	}
}

func HandleUpdateKey(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	//log.Printf("Received update request\n")

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	type p struct {
		Data   map[string][]byte `json:"data"`
		NodeId int               `json:"nodeId"`
	}
	var params p

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	//var data map[string]map[string][]byte
	//if err := json.Unmarshal(params.Data, &data); err != nil {
	//	panic(err)
	//}

	go globalNode.ReceiveUpdate(params.Data, params.NodeId)

	//w.WriteHeader(http.StatusOK)

	log.Printf("done updating key")

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Write the status code to the response
	w.WriteHeader(http.StatusCreated) // Make sure this is the only call to WriteHeader
}

func HandleSetBackup(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string            `json:"key"`
		Value  map[string][]byte `json:"value"`
		NodeId int               `json:"backupNode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err, size := globalNode.SetBackup(kv.Key, kv.Value, kv.NodeId)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare the response structure
	response := struct {
		Size int64 `json:"size"`
	}{
		Size: size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Write the status code to the response
	w.WriteHeader(http.StatusCreated) // Make sure this is the only call to WriteHeader
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// If encoding fails, we cannot call http.Error since the header has already been written
		log.Printf("Error encoding response: %v", err)
		return // Make sure to return here so no more writes occur
	}
}

func HandleGetBackup(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	v, err, size := globalNode.GetBackUp(kv.Key, kv.Fields)

	if err != nil {
		if err.Error() == "redis: nil\n" {
			//log.Printf("Cache miss: %v", err)
			http.Error(w, "redis: nil\n", http.StatusNotFound)
			return
		}
		log.Printf("Error getting key: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare the response structure
	response := struct {
		Value map[string][]byte `json:"value"`
		Size  int64             `json:"size"`
	}{
		Value: v,
		Size:  size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	//
	//
	//
	//
	//if err != nil {
	//	http.Error(w, err.Error(), http.StatusInternalServerError)
	//	return
	//}
	//
	//// Prepare the response structure
	//response := struct {
	//	Value map[string][]byte `json:"value"`
	//	Size  int64             `json:"size"`
	//}{
	//	Value: v,
	//	Size:  size,
	//}
	//
	//// Set the Content-Type header
	//w.Header().Set("Content-Type", "application/json")
	//// Encode and send the response
	//if err := json.NewEncoder(w).Encode(response); err != nil {
	//	http.Error(w, "Error encoding response", http.StatusInternalServerError)
	//	return
	//}
}

func HandleFail(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Fail()
	w.WriteHeader(http.StatusOK)
}

func HandleDone(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	globalCancel()

	if globalNode != nil {
		globalNode.Done()
		globalNode = nil
	}
	w.WriteHeader(http.StatusOK)
}

func HandlePing(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	} else if !globalNode.IsFailed() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func HandleRecover(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Recover(globalNode.Ctx)
	w.WriteHeader(http.StatusOK)
}

var mock = false
var rr = rand.New(rand.NewSource(time.Now().UnixNano()))

func HandleGet(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		log.Printf("Error decoding request body: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var v map[string][]byte = nil
	var size int64 = 0
	var err error = nil

	if mock {
		if rr.Intn(100) >= 92 {
			http.Error(w, "redis: nil\n", http.StatusNotFound)
			return
		}
		v = make(map[string][]byte)
		v["field1"] = []byte("value1")
	} else {

		v, err, size = globalNode.Get(kv.Key, kv.Fields)

		if err != nil {
			if err.Error() == "redis: nil\n" {
				log.Printf("Cache miss: %v", err)
				http.Error(w, "redis: nil\n", http.StatusNotFound)
				return
			}
			log.Printf("Error getting key: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Prepare the response structure
	response := struct {
		Value map[string][]byte `json:"value"`
		Size  int64             `json:"size"`
	}{
		Value: v,
		Size:  size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	} else {
		//w.WriteHeader(http.StatusOK)
	}
}

func HandleSet(w http.ResponseWriter, r *http.Request) {
	if isDone() {
		return
	}
	globalWg.Add(1)
	defer globalWg.Done()

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key        string            `json:"key"`
		Value      map[string][]byte `json:"value"`
		BackUpNode int               `json:"backupNode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	var size int64 = 1
	var err error = nil
	if !mock {
		err, size = globalNode.Set(kv.Key, kv.Value, kv.BackUpNode)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Prepare the response structure
	response := struct {
		Size int64 `json:"size"`
	}{
		Size: size,
	}

	// Set the Content-Type header
	w.Header().Set("Content-Type", "application/json")
	// Write the status code to the response
	w.WriteHeader(http.StatusCreated) // Make sure this is the only call to WriteHeader
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// If encoding fails, we cannot call http.Error since the header has already been written
		log.Printf("Error encoding response: %v", err)
		return // Make sure to return here so no more writes occur
	}

}

var mu sync.Mutex

func NewNodeHandler(w http.ResponseWriter, r *http.Request) {

	mu.Lock()
	defer mu.Unlock()

	if globalNode != nil {
		globalNode.Done()
		globalNode = nil
	}

	if globalCancel != nil {
		globalCancel()
		globalWg.Wait()
	}

	var params struct {
		Id              int     `json:"id"`
		MaxMemMbs       int     `json:"maxMemMbs"`
		MaxMemoryPolicy string  `json:"maxMemoryPolicy"`
		UpdateInterval  float64 `json:"updateInterval"`
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var nodes []string = make([]string, len(config.AllNodes))
	for index := 0; index < len(config.AllNodes); index++ {
		nodes[index] = config.AllNodes[fmt.Sprintf("%d", index)]
	}
	//for index, address := range config.AllNodes {
	//	//if index != fmt.Sprintf("%d", config.Id) {
	//	nodes[index] = address
	//	//}
	//}

	globalCtx, globalCancel = context.WithCancel(context.Background())

	globalNode = node.CreateNewNode(params.Id, config.Redis, params.MaxMemMbs, params.MaxMemoryPolicy, params.UpdateInterval, nodes)
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Node created successfully"))
	if err != nil {
		panic(err)
		return
	}

}
