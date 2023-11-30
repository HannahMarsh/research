FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

# Copy the shared utilities and config file from the shared directory
COPY shared/ ./shared/

# Copy the source code of the cache module
COPY database/ ./

RUN go build -o db

CMD ["./database"]


## Build the Docker image
# ddocker build --platform=linux/amd64 f db/Dockerfile -t hannahmarsh12/db:db .

## Run the Docker container
# docker run -it --rm hannahmarsh12/db:db
