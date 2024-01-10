#!/bin/bash

# Name of the Redis container
REDIS_CONTAINER_NAME="my-redis"
CASSANDRA_CONTAINER_NAME="my-cassandra"

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "Docker does not seem to be running, start it first and rerun this script."
        exit 1
    fi
}

start_redis() {
  local instance=$1
  instance=$((instance + 1))
  local port=$((6379 + instance - 1))
  local name="${REDIS_CONTAINER_NAME}_$instance"
  echo "Starting Redis container $name on port $port..."
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
  echo "Stopping Redis container..."
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
  echo "Removing Redis container..."
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
    echo "Starting Cassandra container..."
    docker run --name $CASSANDRA_CONTAINER_NAME -d --cpus="1.0" -p 9042:9042 cassandra
    echo "Cassandra container started."
}

# Function to stop Redis container
stop_cassandra() {
    echo "Stopping Cassandra container..."
    docker stop $CASSANDRA_CONTAINER_NAME
    echo "Cassandra container stopped."
}

# Function to remove Redis container
remove_cassandra() {
    echo "Removing Cassandra container..."
    docker rm $CASSANDRA_CONTAINER_NAME
    echo "Cassandra container removed."
}

usage() {
  echo "Usage: $0 {start|stop|remove} {cache|db|all} [num_cache_instances]"
  exit 1
}

# Main script logic
check_docker

case "$1" in
    start)
      case "$2" in
          cache)
              num_cache_instances=$3
              start_all_redis "$num_cache_instances"
              ;;
          db)
              start_cassandra
              ;;
          all)
              num_cache_instances=$3
              start_all_redis "$num_cache_instances"
              start_cassandra
              ;;
          *)
              usage
      esac
      ;;
    stop)
        case "$2" in
            cache)
                num_cache_instances=$3
                stop_all_redis "$num_cache_instances"
                ;;
            db)
                stop_cassandra
                ;;
            all)
                num_cache_instances=$3
                stop_all_redis "$num_cache_instances"
                stop_cassandra
                ;;
            *)
                usage
        esac
        ;;
    remove)
        case "$2" in
            cache)
                num_cache_instances=$3
                remove_all_redis "$num_cache_instances"
                ;;
            db)
                remove_cassandra
                ;;
            all)
                num_cache_instances=$3
                remove_all_redis "$num_cache_instances"
                remove_cassandra
                ;;
            *)
                usage
        esac
        ;;
    restart)
        case "$2" in
            cache)
                num_cache_instances=$3
                stop_all_redis "$num_cache_instances"
                remove_all_redis "$num_cache_instances"
                start_all_redis "$num_cache_instances"
                ;;
            db)
                stop_cassandra
                remove_cassandra
                start_cassandra
                ;;
            all)
                num_cache_instances=$3
                stop_all_redis "$num_cache_instances"
                remove_all_redis "$num_cache_instances"
                start_all_redis "$num_cache_instances"
                stop_cassandra
                remove_cassandra
                start_cassandra
                ;;
            *)
                usage
        esac
        ;;
    *)
        usage
esac

echo "Done."
docker ps
