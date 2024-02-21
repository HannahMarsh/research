FROM golang:1.18 as builder

# Set the working directory outside GOPATH to enable the support for modules.
WORKDIR /node

# Copy the go.mod and go.sum to download all dependencies.
COPY go.* ./
RUN go mod download

# Copy the rest of the source code.
COPY . .
COPY node_configs/ /node/node_configs/



# Set the entrypoint to your binary.
ENTRYPOINT ["/node"]


# Set the command to run your binary.
CMD ["./node", "nodeId"]

# Build the application
RUN go build -o node -nodeId <nodeId>

EXPOSE 8081

## Build the Docker image
# docker build -t myclient .
#
## Run the Docker container
# docker run -it --rm myclient

# docker build --platform=linux/amd64 -t hannahmarsh12/client:client .
# docker run -it --rm hannahmarsh12/client:client



## Build the Docker image
# docker build -t mynode .
#
## Run the Docker container
# docker run -it --rm -p <host>:<container> hannahmarsh12/node

# docker build -t mynode .
# docker network create mynetwork
# docker run -it --rm --network mynetwork --name mynode -p 443:443 mynode

# docker build --platform=linux/amd64 -t hannahmarsh12/node .
# docker run -it --rm -p <host>:<container> hannahmarsh12/node


