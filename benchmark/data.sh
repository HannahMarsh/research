#!/bin/bash

copy_file_to_destination() {
    local source_path="$1"
    local destination_path="$2"

    # Extract the directory path from the destination path
    local destination_dir
    destination_dir=$(dirname "$destination_path")

    # Check if the destination directory exists, if not, create it
    if [ ! -d "$destination_dir" ]; then
        echo "Creating directory: $destination_dir"
        mkdir -p "$destination_dir"
    fi

    # Copy the file to the destination path
    echo "Copying $source_path to $destination_path"
    cp "$source_path" "$destination_path"

    echo "Copy completed successfully."
}

WORKLOAD_ID="workload1"

SUMMARY_PATH="data/{$WORKLOAD_ID}_summary.png"
DATA_PATH="data/{$WORKLOAD_ID}"
DESTINATION_DIR="results"

# Generate a folder name with the current date and time
# Format: YYYY-MM-DD_HH-MM-SS
FOLDER_NAME=$(date +"%Y-%m-%d_%H-%M-%S")

# Path to your YAML file
yaml_file="config/default.yaml"

# Use yq to extract the value. Ensure yq version is compatible.
# This command is based on `yq` version 4 syntax.
enable_configuration=$(yq e '.Cache.EnableReconfiguration.Value' "$yaml_file")

# Check if the EnableConfiguration value is true
if [ "$enable_configuration" = "true" ]; then
    echo "EnableConfiguration is true"
    DESTINATION_DIR="$DESTINATION_DIR/with_reconfiguration"
else
    echo "EnableConfiguration is false or not set"
    DESTINATION_DIR="$DESTINATION_DIR/without_reconfiguration"
fi

DESTINATION_DIR="$DESTINATION_DIR/$FOLDER_NAME"

copy_file_to_destination yaml_file "$DESTINATION_DIR/config.yaml"


# Get the current commit hash
commit_hash=$(git rev-parse HEAD)
commit_author=$(git log -1 --pretty=format:'%an')
commit_date=$(git log -1 --pretty=format:'%ad')
commit_message=$(git log -1 --pretty=format:'%s')
commit_info="Hash: $commit_hash
Author: $commit_author
Date: $commit_date
Message: $commit_message"
# Store the commit information in a file
echo "$commit_info" > "$DESTINATION_DIR/commit_info.txt"
echo "Stored commit information:"
echo "$commit_info"

copy_file_to_destination "$SUMMARY_PATH" "$DESTINATION_DIR/summary.png"
copy_file_to_destination "$DATA_PATH" "$DESTINATION_DIR/data"


# Example usage:
# copy_file_to_destination "/path/to/source/file" "/path/to/destination/directory/file"
