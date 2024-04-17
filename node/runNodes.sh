#!/bin/bash

# Initialize variables with default values
node=0
action="build"
cpus="1.0"

usage() {
  echo "Usage: $0 NODE ACTION [OPTIONS]"
  echo ""
  echo "NODE:"
  echo "  0                            Node 0"
  echo "  1                            Node 1"
  echo "  2                            Node 2"
  echo "  3                            Node 3"
  echo "  all                          All nodes"
  echo ""
  echo "ACTIONS:"
  echo "  build                        Build the specified container(s)"
  echo "  run                          Run the specified container(s)"
  echo "  remove                       Remove the specified container(s)"
  echo "  restart                      Restart the specified container(s)"
  echo "  strm                         Stop and remove the specified container(s)"
  echo ""
  echo "OPTIONS:"
  echo "  -cpu -cpus, --cpus           Specify the number of CPUs for each node"
  echo ""
  echo "Examples:"
  echo "  $0 0 build -cpu 1.0          Builds node 0 container with 1 cpu"
  echo ""
  exit 1
}

# Process command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    0|1|2|3|all)
      node="$1"
      shift 1
      ;;
    build|run|remove|restart|strm)
      action="$1"
      shift 1
      ;;
    -cpus|--cpus|-cpu|--cpu)
      cpus="$2"
      shift 2
      ;;
    -h|--h|-help|--help)
      usage
      ;;
    *)
      echo "Invalid argument: $1"
      usage
      ;;
  esac
done

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "Docker does not seem to be running, start it first and rerun this script."
        exit 1
    fi
}

run_redis() {
  local instance=$1
  instance=$((instance + 1))
  local port=$((6379 + instance - 1))
  local name="${REDIS_CONTAINER_NAME}_$instance"
  echo "Starting Redis container $name on port $port..."
  echo "docker run --name $name -d -p $port:6379 redis"
  docker run --name "$name" -d -p $port:6379 redis
  echo "Redis container $name started."
}

# Start all Redis instances
start_all_redis() {
  local num_cache_instances=$1
  for ((i=0; i<num_cache_instances; i++)); do
    start_redis "$i"
  done
}

# Function to stop Redis container
stop_redis() {
  local instance=$1
  instance=$((instance + 1))
  name="${REDIS_CONTAINER_NAME}_$instance"
  echo "docker stop $name"
  docker stop "$name"
  echo "Redis container stopped."
}

stop_all_redis() {
  local num_cache_instances=$1
  for ((i=0;i<num_cache_instances;i++)) do
    stop_redis "$i"
  done
}

# Function to remove Redis container
remove_redis() {
  local instance=$1
  instance=$((instance + 1))
  name="${REDIS_CONTAINER_NAME}_$instance"
  echo "docker rm $name"
  docker rm "$name"
  echo "Redis container removed."
}

remove_all_redis() {
  local num_cache_instances=$1
  for ((i=0;i<num_cache_instances;i++)) do
    remove_redis "$i"
  done
}

# Function to start Cassandra container
start_cassandra() {
    echo "docker run --name $CASSANDRA_CONTAINER_NAME -d --cpus=\"$cpus\" -p 9042:9042 cassandra"
    docker run --name $CASSANDRA_CONTAINER_NAME -d --cpus="$cpus" -p 9042:9042 cassandra
    echo "Cassandra container started."
}

# Function to stop Redis container
stop_cassandra() {
    echo "docker stop $CASSANDRA_CONTAINER_NAME"
    docker stop $CASSANDRA_CONTAINER_NAME
    echo "Cassandra container stopped."
}

# Function to remove Redis container
remove_cassandra() {
    echo "docker rm $CASSANDRA_CONTAINER_NAME"
    docker rm $CASSANDRA_CONTAINER_NAME
    echo "Cassandra container removed."
}

# Main script logic
check_docker

case "$action" in
  build)
      case $node in
        0|1|2|3)
          echo "building image for node $node: hannahmarsh12/node${node}"
          echo "docker build --platform=linux/arm64/v8 -f nodes/node${node}/Dockerfile -t hannahmarsh12/node${node}:node${node} ."
          docker build --platform=linux/arm64/v8 -f "nodes/node${node}/Dockerfile" -t "hannahmarsh12/node${node}:node${node}" .
          echo "done building image for node $node: hannahmarsh12/node${node}"
          ;;
        all)
          for i in 0 1 2 3; do
            node=$i
            echo "building image for node $node: hannahmarsh12/node${node}"
            echo "docker build --platform=linux/arm64/v8 -f nodes/node${node}/Dockerfile -t hannahmarsh12/node${node}:node${node} ."
            docker build --platform=linux/arm64/v8 -f "nodes/node${node}/Dockerfile" -t "hannahmarsh12/node${node}:node${node}" .
            echo "done building image for node $node: hannahmarsh12/node${node}"
          done
          exit 0
          ;;
        *)
          usage
      esac

    ;;
  run)
      port=$((1021 + node))
      echo "running image for node $node. Listening on port $port."
      echo "docker run -it --rm -p $port:$port hannahmarsh12/node${node}:node${node}"
      docker run -it --rm -p $port:$port "hannahmarsh12/node${node}:node${node}"
    ;;
  remove)
      echo "removing container for node $node: hannahmarsh12/node${node}"
      echo "docker rm hannahmarsh12/node${node}:node${node}"
      docker rm "hannahmarsh12/node${node}:node${node}"
      echo "done removing container for node $node: hannahmarsh12/node${node}"
    ;;
  strm)
      echo "stopping container for node $node: hannahmarsh12/node${node}"
      echo "docker stop hannahmarsh12/node${node}:node${node}"
      docker stop "hannahmarsh12/node${node}:node${node}"
      echo "done stopping container for node $node: hannahmarsh12/node${node}"

      echo "removing container for node $node: hannahmarsh12/node${node}"
      echo "docker rm hannahmarsh12/node${node}:node${node}"
      docker rm "hannahmarsh12/node${node}:node${node}"
      echo "done removing container for node $node: hannahmarsh12/node${node}"
    ;;
  restart)
      echo "stopping container for node $node: hannahmarsh12/node${node}"
      echo "docker stop hannahmarsh12/node${node}:node${node}"
      docker stop "hannahmarsh12/node${node}:node${node}"
      echo "done stopping container for node $node: hannahmarsh12/node${node}"

      echo "removing container for node $node: hannahmarsh12/node${node}"
      echo "docker rm hannahmarsh12/node${node}:node${node}"
      docker rm "hannahmarsh12/node${node}:node${node}"
      echo "done removing container for node $node: hannahmarsh12/node${node}"

      port=$((1021 + node))
      echo "running image for node $node. Listening on port $port."
      echo "docker run -it --rm -p $port:$port hannahmarsh12/node${node}:node${node}"
      docker run -it --rm -p $port:$port "hannahmarsh12/node${node}:node${node}"
    ;;
  *)
      usage
esac

echo "Done."
docker ps
