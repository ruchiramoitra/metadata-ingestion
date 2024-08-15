# Use Golang image to build the Go application
FROM --platform=linux/amd64 golang:1.21 AS builder

# Set the working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application
RUN go build -o /metadata-consumer

# Use a smaller image to run the application
FROM alpine:latest

# Copy the built binary from the builder stage
COPY --from=builder /metadata-consumer /metadata-consumer

# Run the binary
CMD ["/metadata-consumer"]