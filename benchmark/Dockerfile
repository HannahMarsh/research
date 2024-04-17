FROM golang:1.18 as builder
LABEL authors="Hannah Marsh"


# Set the working directory outside GOPATH to enable the support for modules.
WORKDIR /node

# Copy the go.mod and go.sum to download all dependencies.
COPY go.* ./
RUN go mod download