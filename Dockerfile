# Stage 1: Build the React UI
FROM node:20-alpine AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm install
COPY ui/ ./
RUN npm run build

# Stage 2: Build the Go binary
FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Copy the built UI from the previous stage
COPY --from=ui-builder /app/ui/dist ./ui/dist
# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o vahak ./cmd/server

# Stage 3: Minimal production image
FROM alpine:latest
WORKDIR /app
# Copy the binary
COPY --from=builder /app/vahak .
# Copy migration files if any exist (golang-migrate uses them)
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080
CMD ["./vahak"]
