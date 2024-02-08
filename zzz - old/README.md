goplantuml \
-hide-private-members \
-recursive \
-show-aggregations \
-show-aliases \
-show-compositions \
-show-connection-labels \
-show-implementations \
"/Users/hanma/cloud computing research/research/b/bench" \
"/Users/hanma/cloud computing research/research/b/cache" \
"/Users/hanma/cloud computing research/research/b/client" \
"/Users/hanma/cloud computing research/research/b/db" \
"/Users/hanma/cloud computing research/research/b/generator" \
"/Users/hanma/cloud computing research/research/b/workload" > "/Users/hanma/cloud computing research/research/b/diagram.puml"


#Quick start
```shell
docker run --name my-cassandra -d --cpus="1.0" -p 9042:9042 cassandra ;
docker run --name my-redis1 -d -p 6379:6379 redis ;
docker run --name my-redis2 -d -p 6380:6379 redis ;
docker ps ;
```

#Quick restart
```shell
docker stop my-cassandra ;
docker stop my-redis1 ;
docker stop my-redis2 ;
docker rm my-cassandra ;
docker rm my-redis1 ;
docker rm my-redis2 ;
docker run --name my-cassandra -d --cpus="1.0" -p 9042:9042 cassandra ;
docker run --name my-redis1 -d -p 6379:6379 redis ;
docker run --name my-redis2 -d -p 6380:6379 redis ;
docker ps ;
```


# Running cassandra with docker

First, make sure the Docker daemon is running.

pull the docker image:
```dockerfile
docker pull cassandra
```

stop existing contianer:
```dockerfile
docker stop my-cassandra
```

remove the stopped contianer:
```dockerfile
docker rm my-cassandra
```

run the new contianer:
```dockerfile
docker run --name my-cassandra -d --cpus="1.0" -p 9042:9042 cassandra
```

verify the contianer is running:
```dockerfile
docker ps
```

check port mapping:
```dockerfile
docker port my-cassandra
```

connect using `cqlsh`:
```dockerfile
docker exec -it my-cassandra cqlsh
```

check logs or warnings:
```dockerfile
docker logs my-cassandra
```

# Running Redis with docker

pull the redis image
```dockerfile
docker pull redis
```

stop existing container:
```dockerfile
docker stop my-redis
```

remove the stopped contianer:
```dockerfile
docker rm my-redis
```

run the new contianer:
```dockerfile
docker run --name my-redis -d --cpus="1.0" -p 6379:6379 redis
```

verify the contianer is running:
```dockerfile
docker ps
```

check port mapping:
```dockerfile
docker port my-redis
```

connect using `cqlsh`:
```dockerfile
docker exec -it my-redis cqlsh
```

check logs or warnings:
```dockerfile
docker logs my-redis
```