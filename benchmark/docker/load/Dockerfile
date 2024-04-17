FROM golang:1.21 as builder
LABEL authors="Hannah Marsh"


# Set the working directory outside GOPATH to enable the support for modules.
WORKDIR /app

# Copy go.mod and go.sum to download all dependencies.
COPY go.* ./
COPY go.*.* ./
RUN go mod download

# Copy the entire source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 go build -o bench ./bench

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Set the working directory in the container
WORKDIR /

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/bench .

# Copy node-specific configurations
COPY config/default.yaml ./config/default.yaml
COPY metrics/fonts/ ./metrics/fonts/

# Set the entrypoint to your binary, include the nodeId as an argument
ENTRYPOINT ["./bench"]
CMD ["load", "-property_file", "config/default.yaml"]

# docker build --platform=linux/arm64/v8 -f docker/load/Dockerfile -t hannahmarsh12/load_bench:load_bench .
# docker run -it --rm hannahmarsh12/load_bench:load_bench