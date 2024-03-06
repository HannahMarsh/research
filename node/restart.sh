#!/bin/bash

numNodes=4

runNode() {
  local nodeId=$1
  go build -o "bin/node${nodeId}" . ;
  # Kill the existing node process before starting a new one
  pkill -f "bin/node${nodeId}"
  # Start the node and detach it (run in the background)
  "./bin/node${nodeId}" -nodeId "${nodeId}" &
}

runNodes() {
  for ((i=1; i<=numNodes; i++)); do
    runNode $i
  done
}

# Make sure the functions are defined before entering the loop
while true; do
  # Listen on port 9099
  { echo -ne "HTTP/1.1 200 OK\r\n\r\n$(date)"; } | nc -l 9099 | while read line; do
    echo "$line" | grep -qE "GET /restart " && runNodes
  done
done
