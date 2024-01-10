#!/bin/bash

usage() {
    echo "Usage: $0 <yaml_file_path>"
    echo "  yaml_file_path: Desired location of the yaml file to be created."
    exit 1
}

# Check for correct number of arguments
if [ "$#" -ne 1 ]; then
    usage
fi

# File paths
GO_FILE="/Users/hanma/cloud computing research/research/b/config/config.go"
YAML_FILE=$1

# Go program to generate YAML from Go source
GO_PROGRAM='
package main

import (
    "fmt"
    "os"
    "gopkg.in/yaml.v3"
    "path/filepath"
    "cloud computing research/research/b/config" // Adjust the import path as necessary
)

func main() {
    defaultConfig := config.NewConfig() // Replace with correct function call if it differs
    yamlData, err := yaml.Marshal(&defaultConfig)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    yamlFilePath := filepath.Clean(os.Args[1])
    err = os.WriteFile(yamlFilePath, yamlData, 0644)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
}
'

# Write the Go program to a temporary file
TMP_GO_PROGRAM=$(mktemp)
echo "$GO_PROGRAM" > "$TMP_GO_PROGRAM"

# Run the Go program to generate the YAML file
go run "$TMP_GO_PROGRAM" "$YAML_FILE"

# Remove the temporary Go program file
rm "$TMP_GO_PROGRAM"

echo "YAML configuration has been generated at $YAML_FILE"
