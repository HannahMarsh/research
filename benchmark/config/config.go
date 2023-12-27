package benchmark_config

import (
	"embed"
	"encoding/json"
	"fmt"
	"strconv"
)

type Config struct {
	value   interface{}
	isEmpty bool
}

//go:embed config.json
var configFile embed.FS

func GetConfig_() (*Config, error) {

	var result interface{}

	fileData, err := configFile.ReadFile("config.json")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(fileData, &result)
	if err != nil {
		return nil, err
	}

	// Accessing top level which is a map
	resultMap, ok := result.(map[string]interface{})
	if ok {
		return &Config{value: resultMap, isEmpty: resultMap == nil}, nil
	} else {
		return nil, fmt.Errorf("failed to get top level of config map")
	}
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
		} else if resultString, ok := c.value.(float64); ok {
			return strconv.Itoa(int(resultString))
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
