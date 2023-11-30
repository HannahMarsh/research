# A Study of Cache-Database Interactions

## resources
- [A Hitchhiker’s Guide to Caching Patterns](https://hazelcast.com/blog/a-hitchhikers-guide-to-caching-patterns/)
  
## repos
- Gocache: [github.com/eko/gocache](https://github.com/eko/gocache)
- Apache Cassandra Client Library: [github.com/gocql/gocql](https://github.com/gocql/gocql)
- Prometheus Client Library: [github.com/prometheus/client_golang](https://github.com/prometheus/client_golang)

## my questions / answers
- What is a cache node?
  > a cache node is an instance of a cache server made out of cache library in a wrapper to facilitate the remote access 
  > to it. It will run on a separate container, which may or may not be on separate machine.
- How will the benchmark determines which node to send requests to?
  > hashing is fine. it is a simple form of load balancing. We know how many caching servers we have, so we can hash keys 
  > into that many "buckets"
- How will we simulate cache node failures?
  > we will fail the cache! essentially we can have a shell script that terminates the container running one of the cache servers  
- Upon detecting a node failure, what is the benchmark's failover strategy?
  > this is your research question for the thesis -- what can we do to reduce the stress on the DB once we see cache 
  > failing. But before we get there, we just need to see it fail and cause a problem
- Is there a cache synchronization protocol to handle the situation when a failed cache node comes back online?
  > at first, the script that failed the cache server can also restart it after some time. This rebooted cache server 
  > will be empty, and that is what we want -- even after reboot, the databases should remain overloaded and cache not filled. 
  > This is because the normal cache-filling strategy for look-aside caches is for the code (benchmark) to set the cache when it 
  > retrieves the data from the main source (DB), but if DB is overloaded, the cache will have hard time filling in
- For request generation, do we want an even distribution of keys, or should we design the generator to produce a skewed distribution where some keys are accessed more frequently ("hotter")?
  > we can try a few strategies. A simpler one is to use zipf distribution for picking keys (and our workload, in this case can 
  > be 100% read operations, so you may need to have a separate warm-up stage the fills the DB with data). A more sophisticate 
  > one is to try to use real traces. I think we want to try both approaches
- What specific metrics are we interested in gathering? 
  > we are interested in latency and throughput observed by the benchmark. Whenever a problem occurs, the latency should go up, 
  > and at some point, when DB falls over (or requests timeout), the throughput will go down
- Is it ok if we use Gocache's built-in Prometheus metrics provider?
  > if the gocache provides additional statistics on cache usage (i.e, # of objects, eviction, rate, etc.) it may be nice to have it

## summary of what i need to do

Create a distributed cache system that interfaces with Apache Cassandra ([gocql](https://github.com/gocql/gocql)) as a datastore and uses [Gocache](https://github.com/eko/gocache) as the
caching layer.   

1. We will have 4 cache nodes that will each be separate Gocache server instances.
  - Gocache has an interface-based approach that will allow us to create our own custom store.  
   - Gocache also offers built-in metrics functionality (like Prometheus integration), which we can use to gather and expose metrics about cache hits, misses, and errors. 
     - We have to configure Gocache to use a metrics provider and the benchmark will then query that provider for the necessary data.

2. Setup Apache Cassandra database on `ccl5.cs.unh.edu`
   - Install it on the server, if not already done.
   - Configure it to allow connections from the cache nodes and benchmark:
     - Locate the cassandra.yaml configuration file, (`/etc/cassandra/` or `/opt/cassandra/conf/`). 
       - Modify `rpc_address` or `listen_address` to allow connections from other machines, setting it to `132.177.10.85` (or `0.0.0.0` for all interfaces).
       - Open the necessary ports (default is 9042 for CQL) on the server's firewall to allow incoming connections.
       - Restart the Cassandra service to apply the changes.
   - Create the necessary keyspaces and tables:
     - Access the Cassandra query shell by running `cqlsh` on the command line.
       - Use a CQL statement to create a keyspace:
           ```sql
           CREATE KEYSPACE mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
           ```
         
3. The benchmark acts as a client, issuing requests and handling node failures while collecting metrics to evaluate the system's 
resilience and performance under different failure scenarios.   

  
### implementation:

- Set up multiple cache node instances, each with a unique address.
- Configure the benchmark client to distribute requests across these nodes.
- Implement failure detection and fallback logic in the benchmark client.
- Set up a metrics collection system using Gocache's integration with Prometheus or another monitoring tool.
- Write a custom request generator that produces the desired mix of read and write operations.
  

## Cache Benchmark Tool
This benchmark tool is designed to simulate a realistic load on a distributed cache system, measuring its performance and 
resilience under various conditions.   
It is tailored for scenarios involving read-heavy workloads and dynamic cache recovery.
  

### Features
- Generates a large number of cache read and write requests to test cache performance under load.
- Simulates failures in cache nodes to analyze the system's failover mechanisms and resilience.
- Integrates with Prometheus to track key metrics like read/write operations, cache hits and misses, operation latency, and throughput.
- Allows adjustment of parameters such as the number of requests, read/write ratios, and cache node details for flexible testing.
- Uses a Zipfian distribution for key selection to simulate real-world access patterns where some keys are hotter than others.

### System Architecture
The benchmark tool interacts with a distributed cache setup comprising multiple cache nodes. It can operate in both local 
and remote configurations. Cache nodes are simulated using `gocache`, a Go-based in-memory cache.
  

### Key Components
- **`config_`:** 
  - Central configuration struct holding database connection details, cache node configurations, and other operational parameters.
- **Metrics Tracking:**
  - **Counters:** `readOpsCounter`, `writeOpsCounter`, `cacheHitsCounter`, `cacheMissesCounter` for tracking various operation counts.
  - **Histogram:** `opLatencyHistogram` for observing the latency of read/write operations.
  - **Gauge:** `throughputGauge` for measuring the operations per second, providing a dynamic view of the system's throughput.
- **Simulated Node Failures:** 
  - A routine that periodically triggers failures and recoveries in cache nodes to test the system's resilience.
- **Throughput Updater:** 
  - A background process that regularly updates the throughput gauge based on the number of successful operations.

### Usage

To run the benchmark:

1.  Configure the cache nodes and database settings in the `config.json` file.
2.  Execute the benchmark program using `go run benchmark.go`. Use flags `-l` for local testing and `-help` for usage instructions.
3.  The tool will start sending requests to the cache nodes, simulating read/write operations.
4.  Prometheus metrics can be accessed at `http://localhost:9100/metrics`.

### Prometheus Integration

The tool exposes various metrics in Prometheus format for easy integration with monitoring systems.  
It provides a detailed view of the cache system's performance, including latency and throughput under normal operation and simulated failure conditions.


## Cache Nodes

### Simulating Cache Failure
An endpoint has been added to the cache server so that, when triggered, it makes the server stop responding to 
regular cache requests. This way, we can simulate a failure without needing to shut down the process.
- `/fail`: When this endpoint is hit, the server changes its state to a "failed" mode where it does not respond to cache requests.
- `/recover`: switches the server back to normal operation.


* * *

## Cache Wrapper  

The Cache Wrapper provides a straightforward interface for cache operations, including setting and 
retrieving values, and simulating node failures and recoveries for testing.

### Functionality  

- Facilitates standard cache operations such as setting (`/set`) and getting (`/get`) values, using Gocache's in-memory store.
- Implements endpoints (`/fail` and `/recover`) to simulate node failures and recoveries.
- Uses a mutex (`failMutex`) to manage concurrent access to the cache's failure state, ensuring thread safety.

### Key Components  

- **Cache Initialization:**
  - **Gocache Client:** Creates an in-memory cache client with a default expiration time and cleanup interval.
  - **Cache Manager:** Initializes a cache manager using Gocache's store, which is central to all cache operations.
  - **HTTP Handlers:**
  - `setupHandlers`: Sets up routes for cache operations and failure simulation.
    - `set_` and `get_`: Handle HTTP requests for setting and retrieving values from the cache.
    - `fail_` and `recover_`: Enable simulation of cache node failures and recoveries.
- **Failure Simulation:**
  - `simulateFailure`: Checks and responds if the cache node is in a failed state.

### Usage  

1.  **Starting the cache node instance:**  
     - Run the cache node using `go run cache.go -id <cache_id> -port <port>`. Replace `<cache_id>` and `<port>` with appropriate values.
       - By default, the cache listens on port `1025`. Use the `-port` flag to change the listening port.  

2.  **Interacting with the cache:**
     - Use HTTP requests to interact with the cache:
       - `GET /get?key=<key>` to retrieve a value.
       - `POST /set?key=<key>&value=<value>` to set a value.  
     
3.  **Simulating failures:**
     - To simulate a failure, send a request to `GET /fail`.
     - To recover the cache, send a request to `GET /recover`.


-------

Outdated readme (need to update):


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
