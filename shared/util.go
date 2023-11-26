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

type Config struct {
	Cache     map[string]NodeConfig `json:"cache"`
	Database  NodeConfig            `json:"database"`
	Benchmark NodeConfig            `json:"benchmark"`
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
