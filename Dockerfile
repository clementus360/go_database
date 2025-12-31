# Stage 1: Build the Go binary
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and sum files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the code
COPY . .

# Build the binary (statically linked for alpine)
RUN CGO_ENABLED=0 GOOS=linux go build -o db-service ./cmd/server/main.go

# Stage 2: Final lightweight image
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/db-service .

# Expose the gRPC port
EXPOSE 50051

# Command to run
CMD ["./db-service"]