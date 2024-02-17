package handlers

import (
	"encoding/json"
	"net/http"
	"node/main/node"
	"strings"
)

var globalNode *node.Node

// params
var RedisAddress string
var OtherNodes stringSlice

// stringSlice is a custom type that satisfies the flag.Value interface.
type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSlice) Set(value string) error {
	*s = strings.Split(value, ",")
	return nil
}

func HandleUpdateKey(w http.ResponseWriter, r *http.Request) {
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
	globalNode = node.CreateNewNode(params.Id, RedisAddress, params.MaxMemMbs, params.MaxMemoryPolicy, OtherNodes)
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Node created successfully"))
	if err != nil {
		panic(err)
		return
	}
}
