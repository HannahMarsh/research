package main

//import (
//	"encoding/json"
//	"fmt"
//	"io"
//	"log"
//	"os"
//)
//
//type Config struct {
//	Id int `json:"id"`
//	Redis string `json:"redis"`
//	ClientPort string            `json:"clientPort"`
//	NodePort string `json:"nodePort"`
//	AllNodes   map[string]string `json:"allNodes"`
//}
//
//func GetConfig(nodeId int) *Config {
//	var config Config
//	fileName := fmt.Sprintf(".../node_configs/config%d.json", nodeId)
//	// Read the JSON file
//	if file, err := os.Open(fileName); err != nil {
//		log.Fatalf("Failed to open %s: %v", fileName, err)
//	} else {
//		defer func(file *os.File) {
//			if err = file.Close(); err != nil {
//				log.Fatalf("Failed to close config file: %v", err)
//			}
//		}(file)
//		if data, err := io.ReadAll(file); err != nil {
//			log.Fatalf("Failed to read config file: %s", err)
//		} else if err = json.Unmarshal(data, &config); err != nil {
//			log.Fatalf("Error unmarshaling JSON: %v", err)
//		}
//	}
//	return &config
//}
