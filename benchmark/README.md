
# Running cassandra with docker

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

