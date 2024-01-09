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
spaces2=$(add_spaces "$PROPERTY_NAME" "$((longest_prop_length))")
ymlspaces=$(add_spaces "$TYPE" 9)

# Update the Go file
# Add the new property to the Config struct with correct spacing
sed -i '' "/type Config struct {/a\\
    $PROPERTY_NAME$spaces$TYPE$ymlspaces\`yaml:\"$PROPERTY_NAME\"\`\\
" "$GO_FILE"

# Add the new property with its default value to the defaultConfig in NewConfig function, with a newline after
sed -i '' "/defaultConfig := Config{/a\\
        $PROPERTY_NAME:$spaces2$DEFAULT_VALUE,\\
" "$GO_FILE"

# Update the YAML file
# Add the new property with its value and a newline after
echo "$PROPERTY_NAME: $DEFAULT_VALUE" >> "$YAML_FILE"
echo "" >> "$YAML_FILE"

echo "Property '$PROPERTY_NAME' added successfully."


rm "${GO_FILE}.bak.sorted"
rm "${GO_FILE}.bak"
rm "${GO_FILE}.bak.sorted.sorted"

# Backup original file
cp "$GO_FILE" "${GO_FILE}.bak"

sort_from() {

  local FILE="$1"
  local from="$2"

  # Get the start line of the Config struct
  start_line=$(grep -n "$from" "$FILE" | cut -d: -f1)


  # Get the end line of the Config struct
  # This assumes the closing brace of the struct is at the start of the line
  # Get the end line of the Config struct
  end_line=$(awk -v start="$start_line" 'NR >= start {
      if ($0 ~ /{/) { brace_count++ }
      if ($0 ~ /}/) { brace_count-- }
      if (brace_count == 0 && NR > start) { print NR; exit }
  }' "$FILE")

  start_line=$((start_line + 1))
  end_line=$((end_line - 1))

  echo "$start_line to $end_line"
  # Temporary files for storing parts of the original file
  TEMP_BEFORE="${FILE}_temp_before.txt"
  TEMP_SORTED="${FILE}_temp_sorted.txt"
  TEMP_AFTER="${FILE}_temp_after.txt"

  # Extract lines before the sort range
  T=$((start_line - 1))
  head -n "$T" "$FILE" > "$TEMP_BEFORE"

  # Extract and sort lines within the sort range
  sed -n "${start_line},${end_line}p" "$FILE" | sort -f -b > "$TEMP_SORTED"

  # Extract lines after the sort range
  T=$((end_line + 1))
  tail -n "+$T" "$FILE" > "$TEMP_AFTER"

  # Concatenate the parts and overwrite the original file
  cat "$TEMP_BEFORE" "$TEMP_SORTED" "$TEMP_AFTER" > "$FILE.sorted"

  # Clean up temporary files
  rm "$TEMP_BEFORE" "$TEMP_SORTED" "$TEMP_AFTER"

  echo "File '$FILE' has been sorted from line $start_line to $end_line and updated."
}

sort_from "${GO_FILE}.bak" "type Config struct {"
sort_from "${GO_FILE}.bak.sorted" "defaultConfig := Config{"
cp "${GO_FILE}.bak.sorted.sorted" "$GO_FILE"
rm "${GO_FILE}.bak.sorted.sorted"
rm "${GO_FILE}.bak.sorted"
rm "${GO_FILE}.bak"




