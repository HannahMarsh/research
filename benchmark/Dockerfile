FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

# Copy the shared utilities and config file from the shared directory
COPY shared/ ./shared/

# Copy the source code of the cache module
COPY benchmark/ ./

RUN go build -o benchmark

CMD ["./benchmark"]


## Build the Docker image
# ddocker build --platform=linux/amd64 -f benchmark/Dockerfile -t hannahmarsh12/benchmark:benchmark .

## Run the Docker container
# docker run -it --rm hannahmarsh12/benchmark:benchmark