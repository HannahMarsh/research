package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {

	time.Sleep(2 * time.Second)

	nodes := []int{8081, 8082}

	for id, n := range nodes {
		simulateNewNode(n, id)
	}

	time.Sleep(2 * time.Second)

	kv := make(map[string]map[string][]byte)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		kv[key] = map[string][]byte{"field1": []byte(value + "-1"), "field2": []byte(value + "-2")}
		time.Sleep(5 * time.Millisecond)
	}
	count := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		if count%2 == 0 {
			simulateSet(nodes[0], key, kv[key], 1)
		} else {
			simulateSet(nodes[1], key, kv[key], 0)
		}
		count++
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	count = 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := kv[key]

		var result map[string][]byte

		var resp map[string]map[string][]byte
		if count%2 == 0 {
			resp = simulateGet(nodes[0], key)
		} else {
			resp = simulateGet(nodes[1], key)
		}

		if resp != nil && resp["value"] != nil {
			result = resp["value"]
		}

		if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", expected) {
			fmt.Printf("key: %s, result: %v, \n\t  expected: %v\n", key, result, expected)
		} else {
			fmt.Printf("key: %s, matched! result: %v\n", key, result)
		}

		count++
		time.Sleep(5 * time.Millisecond)
	}
}

func sendRequest(method, url string, payload []byte) (string, int) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return "", -1
	}

	// Set the Content-Type header only if there's a payload
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return "", -1
	}
	defer resp.Body.Close() // Simplified defer statement

	// Read the entire response body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return "", -1
	}

	if resp.StatusCode == 500 {
		log.Printf("Received response from %s: %v: %s\n", url, resp.StatusCode, resp.Status)
	}

	return string(b), resp.StatusCode
}

func simulateNewNode(port int, id int) {
	jsonPayload := []byte(fmt.Sprintf("{\"id\": %d, \"maxMemMbs\": 256, \"maxMemoryPolicy\": \"allkeys-lru\"}", id))
	sendRequest("POST", fmt.Sprintf("http://localhost:%d/newNode", port), jsonPayload)
}

func simulateGet(port int, key string) map[string]map[string][]byte {

	type kv struct {
		Key    string   `json:"key"`
		Fields []string `json:"fields"`
	}
	var jsonPayload = kv{
		Key:    key,
		Fields: make([]string, 0),
	}

	jsonPayloadBytes, err := json.Marshal(jsonPayload)
	if err != nil {
		panic(err)
	}

	// This is a GET request, so no payload is sent
	response, status := sendRequest("GET", fmt.Sprintf("http://localhost:%d/get", port), []byte(jsonPayloadBytes))
	if status != http.StatusOK {
		if response == "redis: nil\n" {
			log.Printf("cache miss: %s\n", key)
		} else {
			log.Printf("Received response from %s: %v: %s\n", fmt.Sprintf("http://localhost:%d/get", port), status, response)
		}
		return nil
	}
	if response == "" {
		return nil
	}

	// Define a structure to unmarshal the JSON string
	var data struct {
		Value map[string]string `json:"value"`
		Size  int               `json:"size"`
	}

	// Unmarshal the JSON string
	if err := json.Unmarshal([]byte(response), &data); err != nil {
		log.Fatal(err)
	}

	// Convert the string values in the map to []byte
	result := make(map[string]map[string][]byte)
	result["value"] = make(map[string][]byte)
	for key, valueStr := range data.Value {
		decodedBytes, err := base64.StdEncoding.DecodeString(valueStr)
		if err != nil {
			log.Fatal(err)
		}
		result["value"][key] = decodedBytes
	}
	return result
}

func simulateSet(port int, key string, value map[string][]byte, backupNode int) {

	type kv struct {
		Key        string            `json:"key"`
		Value      map[string][]byte `json:"value"`
		BackUpNode int               `json:"backupNode"`
	}

	jsonPayload, err := json.Marshal(kv{Key: key, Value: value, BackUpNode: backupNode})
	if err != nil {
		panic(err)
	}
	sendRequest("POST", fmt.Sprintf("http://localhost:%d/set", port), []byte(jsonPayload))
}

func simulateFail(port int) {
	// Sending a POST request to simulate a fail action
	sendRequest("POST", fmt.Sprintf("http://localhost:%d/fail", port), nil)
}

func simulateRecover(port int) {
	// Sending a POST request to simulate a recover action
	sendRequest("POST", fmt.Sprintf("http://localhost:%d/recover", port), nil)
}
