FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

# Copy the shared utilities and config file from the shared directory
COPY shared/ ./shared/

# Copy the source code of the cache module
COPY database/ ./

RUN go build -o database

CMD ["./database"]


## Build the Docker image
# ddocker build --platform=linux/amd64 f database/Dockerfile -t hannahmarsh12/database:database .

## Run the Docker container
# docker run -it --rm hannahmarsh12/database:database
