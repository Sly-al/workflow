# Build stage
FROM golang:1.24.0-alpine AS builder

WORKDIR /app

# Install necessary build tools
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/workflow ./cmd/main.go

# Final stage
FROM alpine:3.19

WORKDIR /app

# Install necessary runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy the binary from builder
COPY --from=builder /app/workflow .

# Create a non-root user
RUN adduser -D -g '' appuser
USER appuser

# Expose the application port
EXPOSE 8080

# Run the application
CMD ["./workflow"] 