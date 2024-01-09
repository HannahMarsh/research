#!/bin/bash

# Check for correct number of arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <property_name> <type> <default_value>"
    exit 1
fi

PROPERTY_NAME=$1
TYPE=$2
DEFAULT_VALUE=$3

# File paths
GO_FILE="/Users/hanma/cloud computing research/research/b/config/config.go"
YAML_FILE="/Users/hanma/cloud computing research/research/b/tool/property_files/0.yaml"

# Update the Go file
# Add the new property to the Config struct
sed -i '' "/type Config struct/a\\
    $PROPERTY_NAME $TYPE \`yaml:\"$PROPERTY_NAME\"\`" "$GO_FILE"

# Add the new property to the defaultConfig in NewConfig function
sed -i '' "/defaultConfig := Config{/a\\
        $PROPERTY_NAME: $DEFAULT_VALUE," "$GO_FILE"

# Update the YAML file
echo "$PROPERTY_NAME: $DEFAULT_VALUE" >> "$YAML_FILE"

echo "Property '$PROPERTY_NAME' added successfully."
