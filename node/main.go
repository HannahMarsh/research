package node

import (
	"encoding/json"
	"net/http"
)

const clientPort = "8080"
const otherNodePort = "8081"

var globalNode *Node

func main() {
	go serveClients()
	go serveOtherNodes()
}

func serveClients() {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register handler functions for different paths
	mux.HandleFunc("/newNode", newNodeHandler)
	mux.HandleFunc("/get", handleGet)
	mux.HandleFunc("/getBackup", handleGetBackup)
	mux.HandleFunc("/set", handleSet)
	mux.HandleFunc("/setBackup", handleSetBackup)
	mux.HandleFunc("/fail", handleFail)
	mux.HandleFunc("/recover", handleRecover)
	mux.HandleFunc("/ping", handlePing)
	mux.HandleFunc("/done", handleDone)

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
	mux.HandleFunc("/updateKey", handleUpdateKey)

	// Start the server with the mux as the handler
	err := http.ListenAndServe(":"+otherNodePort, mux)
	if err != nil {
		panic(err)
	}
}

func handleSetBackup(w http.ResponseWriter, r *http.Request) {
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

func handleGetBackup(w http.ResponseWriter, r *http.Request) {
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

func handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	var params struct {
		Data   []byte `json:"value"`
		NodeId int    `json:"nodeId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var data map[string]map[string][]byte
	if err := json.Unmarshal(params.Data, &data); err != nil {
		panic(err)
	}

	globalNode.ReceiveUpdate(data, params.NodeId)

	w.WriteHeader(http.StatusOK)
}

func handleFail(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Fail()
	w.WriteHeader(http.StatusOK)
}

func handleDone(w http.ResponseWriter, r *http.Request) {
	if globalNode != nil {
		globalNode.Done()
		globalNode = nil
	}
	w.WriteHeader(http.StatusOK)
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	} else if !globalNode.isFailed {
		w.WriteHeader(http.StatusOK)
	}
}

func handleRecover(w http.ResponseWriter, r *http.Request) {
	if globalNode == nil {
		http.Error(w, "No node available", http.StatusServiceUnavailable)
		return
	}
	globalNode.Recover(globalNode.ctx)
	w.WriteHeader(http.StatusOK)
}

// ServeHTTP serves the cache node over HTTP
func newNodeHandler(w http.ResponseWriter, r *http.Request) {
	var params struct {
		Id              int      `json:"id"`
		Address         string   `json:"address"`
		MaxMemMbs       int      `json:"maxMemMbs"`
		MaxMemoryPolicy string   `json:"maxMemoryPolicy"`
		OtherNodes      []string `json:"otherNodes"`
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	globalNode = CreateNewNode(params.Id, params.Address, params.MaxMemMbs, params.MaxMemoryPolicy, params.OtherNodes)
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Node created successfully"))
	if err != nil {
		panic(err)
		return
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
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

	v, err, size := globalNode.Get(kv.Key, kv.Fields)

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

func handleSet(w http.ResponseWriter, r *http.Request) {
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
	// Encode and send the response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
