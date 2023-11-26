FROM golang:1.21
LABEL authors="Hannah Marsh"

WORKDIR /app

# Copy the Go module files for the cache module and download dependencies
COPY cache/go.mod cache/go.sum ./
RUN go mod download

# Copy the shared utilities and config file from the shared directory
COPY shared/ ./

# Copy the source code of the cache module
COPY cache/ ./

# Build the application
RUN go build -o cache

# Command to run the executable
CMD ["./cache"]

# Build the Docker image
# docker build --platform=linux/amd64 -t hannahmarsh12/cache:cache .

# Run the Docker container
# docker run -it --rm hannahmarsh12/cache:cache
