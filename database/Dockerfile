FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

COPY go.mod .

RUN go mod download

COPY . .

RUN go build -o database

CMD ["./database"]


## Build the Docker image
# ddocker build --platform=linux/amd64 -t hannahmarsh12/database:database .

## Run the Docker container
# docker run -it --rm hannahmarsh12/database:database
