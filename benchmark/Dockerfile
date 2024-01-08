FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

# Copy the source code of the cache module
COPY bench/ ./

RUN go build -o benchmark

CMD ["./benchmark"]


## Build the Docker image
# ddocker build --platform=linux/amd64 -f benchmark/Dockerfile -t hannahmarsh12/benchmark:benchmark .

## Run the Docker container
## mount the directory containing the traces into the Docker container
# docker run -v /usr/local/share/datasets/cmu-cache-datasets/alibabaBlock:/app/traces -it --rm hannahmarsh12/benchmark:benchmark