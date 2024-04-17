# Use the official Golang base image for the build stage
FROM golang:1.18 as builder

# Set the working directory outside GOPATH to enable the support for modules.
WORKDIR /app

# Copy go.mod and go.sum to download all dependencies.
COPY go.* ./
COPY go.*.* ./
RUN go mod download

# Copy the entire source code
COPY main/ ./main/

# Build the application
RUN CGO_ENABLED=0 go build -o main ./main

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Set the working directory in the container
WORKDIR /

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/main .

# Copy node-specific configurations
COPY nodes/node1/config.json ./nodes/node1/config.json

# Expose the port (should match the port used by your app, configurable via the config file or command line)
EXPOSE 1022

# Set the entrypoint to your binary, include the nodeId as an argument
ENTRYPOINT ["./main"]
CMD ["-nodeId", "1"]


# docker build --platform=linux/arm64/v8 -f nodes/node1/Dockerfile -t hannahmarsh12/node1:node1 .
# docker run -it --rm -p 1022:1022 hannahmarsh12/node1:node1