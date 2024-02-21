package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"node/main/node"
)

var globalNode *node.Node

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

func HandleUpdateKey(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received update request\n")

	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var params struct {
		Data   map[string]map[string][]byte `json:"value"`
		NodeId int                          `json:"nodeId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	//var data map[string]map[string][]byte
	//if err := json.Unmarshal(params.Data, &data); err != nil {
	//	panic(err)
	//}

	globalNode.ReceiveUpdate(params.Data, params.NodeId)

	w.WriteHeader(http.StatusOK)
}

func HandleSetBackup(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string            `json:"key"`
		Value  map[string][]byte `json:"value"`
		NodeId int               `json:"nodeId"`
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
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func HandleGetBackup(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
		NodeId int      `json:"nodeId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	v, err, size := globalNode.GetBackUp(kv.Key, kv.Fields, kv.NodeId)

	if err != nil {
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
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func HandleFail(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Fail()
	w.WriteHeader(http.StatusOK)
}

func HandleDone(w http.ResponseWriter, r *http.Request) {
	if globalNode != nil {
		globalNode.Done()
		globalNode = nil
	}
	w.WriteHeader(http.StatusOK)
}

func HandlePing(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	} else if !globalNode.IsFailed() {
		w.WriteHeader(http.StatusOK)
	}
}

func HandleRecover(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Recover(globalNode.Ctx)
	w.WriteHeader(http.StatusOK)
}

func HandleGet(w http.ResponseWriter, r *http.Request) {
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

	v, err, size := globalNode.Get(kv.Key, kv.Fields)

	if err != nil {
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
}

func HandleSet(w http.ResponseWriter, r *http.Request) {
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

	err, size := globalNode.Set(kv.Key, kv.Value, kv.BackUpNode)

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

func NewNodeHandler(w http.ResponseWriter, r *http.Request) {
	var params struct {
		Id              int    `json:"id"`
		MaxMemMbs       int    `json:"maxMemMbs"`
		MaxMemoryPolicy string `json:"maxMemoryPolicy"`
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var nodes []string
	for index, address := range config.AllNodes {
		if index != fmt.Sprintf("%d", config.Id) {
			nodes = append(nodes, address)
		}
	}
	globalNode = node.CreateNewNode(params.Id, config.Redis, params.MaxMemMbs, params.MaxMemoryPolicy, nodes)
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Node created successfully"))
	if err != nil {
		panic(err)
		return
	}
}
