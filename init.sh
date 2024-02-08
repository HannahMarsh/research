#!/bin/bash

# Name of the Redis container
REDIS_CONTAINER_NAME="my-redis"
CASSANDRA_CONTAINER_NAME="my-cassandra"

# Initialize variables with default values
num_cache_instances=4
action="start"
container="all"
cpus="1.0"

usage() {
  echo "Usage: $0 ACTION [OPTIONS]"
  echo ""
  echo "ACTIONS:"
  echo "  start                        Start the specified container(s)"
  echo "  stop                         Stop the specified container(s)"
  echo "  remove                       Remove the specified container(s)"
  echo "  restart                      Restart the specified container(s)"
  echo "  strm                         Stop and remove the specified container(s)"
  echo ""
  echo "OPTIONS:"
  echo "  -c -container, --container   Specify the container type (cache, db, all). Default: all"
  echo "  -n -nodes, --nodes           Specify the number of cache instances (applicable to cache container). Default: 4"
  echo "  -cpu -cpus, --cpus           Specify the number of CPUs for Cassandra container. Default: 1.0"
  echo ""
  echo "Examples:"
  echo "  $0 start cache -n 3          Start Redis containers with 3 instances"
  echo "  $0 stop db                   Stop Cassandra container"
  echo "  $0 restart all -cpus 2.0     Restart all containers with cassandra running on 2 CPUs"
  echo ""
  exit 1
}

# Process command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    start|stop|remove|restart|strm)
      action="$1"
      shift 1
      ;;
    db|all|cache|redis|cassandra)
      container="$1"
      shift 1
      ;;
    -nodes|--nodes|-n|--n)
      num_cache_instances="$2"
      shift 2
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

start_redis() {
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
  start)
    case "$container" in
      cache|redis)
          start_all_redis "$num_cache_instances"
          ;;
      db|cassandra)
          start_cassandra
          ;;
      all)
          start_all_redis "$num_cache_instances"
          start_cassandra
          ;;
      *)
          usage
    esac
    ;;
  stop)
    case "$container" in
      cache|redis)
          stop_all_redis "$num_cache_instances"
          ;;
      db|cassandra)
          stop_cassandra
          ;;
      all)
          stop_all_redis "$num_cache_instances"
          stop_cassandra
          ;;
      *)
          usage
    esac
    ;;
  remove)
    case "$container" in
      cache|redis)
          remove_all_redis "$num_cache_instances"
          ;;
      db|cassandra)
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
  strm)
    case "$container" in
          cache|redis)
              stop_all_redis "$num_cache_instances"
              remove_all_redis "$num_cache_instances"
              ;;
          db|cassandra)
              stop_cassandra
              remove_cassandra
              ;;
          all)
              stop_all_redis "$num_cache_instances"
              remove_all_redis "$num_cache_instances"
              stop_cassandra
              remove_cassandra
              ;;
          *)
              usage
        esac
        ;;
  restart)
    case "$container" in
      cache|redis)
          stop_all_redis "$num_cache_instances"
          remove_all_redis "$num_cache_instances"
          start_all_redis "$num_cache_instances"
          ;;
      db|cassandra)
          stop_cassandra
          remove_cassandra
          start_cassandra
          ;;
      all)
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
