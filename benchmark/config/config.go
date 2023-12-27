package benchmark_config

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type Config struct {
	value   interface{}
	isEmpty bool
}

func GetConfig_() *Config {

	var result interface{}

	jsonFile, err := os.Open("config.json")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			log.Fatalf("Failed to close config file: %s", err)
		}
	}(jsonFile)

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	// Unmarshal the byte value into the interface
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		fmt.Println(err)
	}

	// Example: Accessing data assuming top level is a map
	if resultMap, ok := result.(map[string]interface{}); ok {
		return &Config{value: resultMap, isEmpty: resultMap == nil}
	}
	return nil
}

func (c *Config) Get(key string) *Config {
	if c.isEmpty {
		return c
	}
	if resultMap, ok := c.value.(map[string]interface{}); ok {
		return &Config{value: resultMap[key], isEmpty: resultMap[key] == nil}
	} else {
		fmt.Printf("Did not find key %s in config file\n\n", key)
		return &Config{value: nil, isEmpty: true}
	}
}

func (c *Config) AsString(dflt string) string {
	if !c.isEmpty {
		if resultString, ok := c.value.(string); ok {
			return resultString
		}
	}
	fmt.Printf("Can not convert to string\n")
	return dflt
}

func (c *Config) AsInt(dflt int) int {
	return int(c.AsFloat(float64(dflt)))
}

func (c *Config) AsFloat(dflt float64) float64 {
	if !c.isEmpty {
		if resultFloat, ok := c.value.(float64); ok {
			return resultFloat
		}
	}
	fmt.Printf("Can not convert to float\n")
	return dflt
}

func (c *Config) AsBool(dflt bool) bool {
	if !c.isEmpty {
		if resultBool, ok := c.value.(bool); ok {
			return resultBool
		}
	}
	fmt.Printf("Can not convert to bool\n")
	return dflt
}
