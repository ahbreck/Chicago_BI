# syntax=docker/dockerfile:1
FROM golang:1.24-alpine
WORKDIR /app

# Copy go module files and download deps
COPY go.mod ./
COPY go.sum ./
RUN go mod tidy

# Copy source code
COPY . ./

# Build the binary
RUN go build -o /main

# Run the app
CMD ["/main"]
