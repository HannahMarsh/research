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


## Simulating Cache Failure
An endpoint has been added to the cache server so that, when triggered, it makes the server stop responding to 
regular cache requests. This way, we can simulate a failure without needing to shut down the process.
- `/fail`: When this endpoint is hit, the server changes its state to a "failed" mode where it does not respond to cache requests.
- `/recover`: switches the server back to normal operation.


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
