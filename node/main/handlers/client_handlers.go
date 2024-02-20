package handlers

import (
	"encoding/json"
	"log"
	"net/http"
)

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
