# Use the official Golang image as the build environment
FROM golang:1.23 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to the working directory
COPY go.mod go.sum ./

# Download the dependencies
RUN go mod download

# Copy the rest of the application code to the working directory
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -o /submission-sequencer-finalizer ./cmd/main.go

# Use a minimal base image
FROM scratch

# Copy the binary from the builder stage
COPY --from=builder /submission-sequencer-finalizer /submission-sequencer-finalizer

# Command to run the application
CMD ["/submission-sequencer-finalizer"]
