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

# Function to add spaces for alignment
add_spaces() {
    local prop_name="$1"
    local longest_prop_length="$2"

    local current_length=${#prop_name}
    local spaces_to_add=$((longest_prop_length - current_length))

    local spaces=""
    for ((i=0; i<spaces_to_add - 1; i++)); do
        spaces+=" "
    done

    echo "$spaces"
}

# Determine the longest property name for alignment
# Updated to use a compatible regex for BSD grep
longest_prop_length=$(grep -o '^[[:blank:]]*[a-zA-Z_][a-zA-Z0-9_]*' "$GO_FILE" | awk '{ print length }' | sort -nr | head -1)

# Spaces for new property
spaces=$(add_spaces "$PROPERTY_NAME" "$longest_prop_length")
ymlspaces=$(add_spaces "$TYPE" 9)

# Update the Go file
# Add the new property to the Config struct with correct spacing
sed -i '' "/type Config struct/a\\
    $PROPERTY_NAME$spaces$TYPE$ymlspaces\`yaml:\"$PROPERTY_NAME\"\`\\
" "$GO_FILE"

# Add the new property with its default value to the defaultConfig in NewConfig function, with a newline after
sed -i '' "/defaultConfig := Config{/a\\
        $PROPERTY_NAME: $DEFAULT_VALUE,\\
" "$GO_FILE"

# Update the YAML file
# Add the new property with its value and a newline after
echo "$PROPERTY_NAME: $DEFAULT_VALUE" >> "$YAML_FILE"
echo "" >> "$YAML_FILE"

echo "Property '$PROPERTY_NAME' added successfully."

# Extract and sort the properties
sorted_properties=$(sed -n '/type Config struct {/,/}/p' "$GO_FILE" | grep '^[[:blank:]]*[a-zA-Z_]' | sort)

# Replace the struct with sorted properties
awk -v sorted="$sorted_properties" '
    /type Config struct {/ {
        print;
        print sorted;
        skip = 1;
    }
    /}/ {
        if (skip) {
            print;
            skip = 0;
        }
    }
    !skip { print }' "$GO_FILE" > "$GO_FILE".tmp && mv "$GO_FILE".tmp "$GO_FILE"

echo "Properties in 'Config' struct sorted alphabetically."
