# A Study of Cache-Database Interactions

## To do
- replace database module with a real one (Apache Cassandra, for ex.)


## Description

This project is focused on benchmarking distributed systems, specifically investigating how cache and database interactions impact system stability.  
The research emphasizes scenarios of metastable states that arise from cache node failure and aims to provide insights into the behavior of systems under various load conditions.
It utilizes real-world trace data to simulate workload and analyze system performance.


## System Configuration
- 5 servers - one for the database and four for cache instances.
- The database runs in its own container using 2 cores.
- Each cache instance uses 3 cores, totaling 12 cores for all cache instances.
- Aims for 90% of requests to be served by the cache and 10% by the database.

## Project Structure

The project consists of multiple modules, each with its own functionality:

- **Cache Node**: Manages caching operations (uses Memcache)
- **Database**: Manages database operations.
- **Benchmark**: Responsible for distributing requests between cache and database layers and uses Alibaba traces for performance analysis.
- **Shared**: Contains shared utilities and configuration logic.  

## Benchmarking with Trace Files
- Trace files are located in the `/usr/local/share/datasets/cmu-cache-datasets/alibabaBlock` directory on the `ccl5` remote host and are used by the benchmark module to simulate realistic workload patterns.
- The benchmark container will run on this remote host and will have the `alibabaBlock` directory mounted to `/app/traces` inside the Docker container.


## Simulating a Cache Failure
- Intentionally disable one cache instance to observe the effects on the database.
- Recovery Analysis: Study the system's behavior when the cache instance is reinstated.

## Getting Started

### Installation

To build and run the Docker containers for each module, follow these steps:

#### Building the Docker Image

Navigate to the project root and build the Docker image for each module. For example, to build the benchmark module:

`docker build --platform=linux/amd64 -f benchmark/Dockerfile -t hannahmarsh12/benchmark:benchmark .`

Repeat similar steps for the cache and database modules.


#### Running the Docker Container with Trace File Access

To run the benchmark module with access to the trace files, use the following command:

`docker run -v /usr/local/share/datasets/cmu-cache-datasets/alibabaBlock:/app/traces -it --rm hannahmarsh12/benchmark:benchmark`

This command mounts the `alibabaBlock` directory containing the trace files into the container at `/app/traces`.

### Usage

- The benchmark module will process the trace files located in `/app/traces` inside the Docker container.
- Make sure the benchmark application logic refers to this path when accessing the trace files.

## Authors

- **Hannah Marsh** - _Initial work_ - [hrm1065](https://gitlab.cs.unh.edu/hrm1065)
