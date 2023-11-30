# A Study of Cache-Database Interactions

## resources
- [A Hitchhiker’s Guide to Caching Patterns](https://hazelcast.com/blog/a-hitchhikers-guide-to-caching-patterns/)
  
## repos
- Gocache: [github.com/eko/gocache](https://github.com/eko/gocache)
- Apache Cassandra Client Library: [github.com/gocql/gocql](https://github.com/gocql/gocql)
- Prometheus Client Library: [github.com/prometheus/client_golang](https://github.com/prometheus/client_golang)

## my notes / questions
- A cache node will be an instance of a cache server, running on a separate host machine with unique ip/port
- How will the benchmark determines which node to send requests to?
  - based on a load balancing strategy or some sort of hashing mechanism?
- How will we simulate cache node failures?
  - Are these failures to be artificially triggered by the benchmark? 
    - If not, how will the benchmark detect when a node failure occurs?
      - Is a health-check or timeout mechanism in place for this? 
    - Upon detecting a node failure, what is the benchmark's failover strategy? 
      - Should it attempt to reroute to an alternate cache node or fall back to the database directly? 
    - Is there a cache synchronization protocol to handle the situation when a failed cache node comes back online?
      - If so, what layer will this be implemented at?
- For request generation, do we want an even distribution of keys, or should we design the generator to produce a skewed distribution where some keys are accessed more frequently ("hotter")?
- Should the generator use a deterministic or a stochastic model to simulate the requests (assuming 99% reads, 1% writes)?
- What specific metrics are we interested in gathering? 
  - Is it ok if we use Gocache's built-in Prometheus metrics provider?
  - Should the database/benchmark also collect results like latency/throughput?

## summary of what i need to do

1. Create a distributed cache system that interfaces with Apache Cassandra ([gocql](https://github.com/gocql/gocql)) as a datastore and uses [Gocache](https://github.com/eko/gocache) as the
caching layer.  
   - Gocache has an interface-based approach that will allow us to create our own custom store.  
   - Gocache also offers built-in metrics functionality (like Prometheus integration), which we can use to gather and expose metrics about cache hits, misses, and errors. 
     - We have to configure Gocache to use a metrics provider and the benchmark will then query that provider for the necessary data.

2. We will have 4 cache nodes that will each be separate cache instances that we can manage and orchestrate.   

3. The benchmark acts as a client, issuing requests and handling node failures while collecting metrics to evaluate the system's 
resilience and performance under different failure scenarios.   

  
### implementation:

- Set up multiple cache node instances, each with a unique address.
- Configure the benchmark client to distribute requests across these nodes.
- Implement failure detection and fallback logic in the benchmark client.
- Set up a metrics collection system using Gocache's integration with Prometheus or another monitoring tool.
- Write a custom request generator that produces the desired mix of read and write operations.



-------



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
