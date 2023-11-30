package shared

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type NodeConfig struct {
	IP   string `json:"ip"`
	Port string `json:"port"`
}

type DieInfo struct {
	Time     int64 `json:"time"`
	Duration int64 `json:"duration"`
}

type CacheConfig struct {
	IP   string    `json:"ip"`
	Port string    `json:"port"`
	Die  []DieInfo `json:"die"` // times to fail to how long to fail for
}

type Config struct {
	Cache     map[string]CacheConfig `json:"cache"`
	Database  NodeConfig             `json:"db"`
	Benchmark NodeConfig             `json:"benchmark"`
}

func LoadConfig() Config {

	// Open the JSON file
	file, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Failed to open config file: %s", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close config file: %s", err)
		}
	}(file)

	// Read the file
	data, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	// Define a Config instance
	var config Config

	// Unmarshal the JSON data into the Config struct
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling JSON: %v", err)
	}

	return config
}
