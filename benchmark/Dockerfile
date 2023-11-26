FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

COPY . .

RUN go build -o benchmark

CMD ["./benchmark"]


## Build the Docker image
# ddocker build --platform=linux/amd64 -t hannahmarsh12/benchmark:benchmark .

## Run the Docker container
# docker run -it --rm hannahmarsh12/benchmark:benchmark