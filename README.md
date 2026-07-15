# Vahak 🦅

Vahak is a webhook gateway and integration engine written in Go. It acts as a buffer between external webhook providers and internal services, providing reliable delivery, payload transformation, and real-time observability.

## 🏗️ Architecture & Features

- ⚡ **Hybrid Queue System**: Uses a buffered Go channel (`queue.JobQueue`) for immediate, non-blocking webhook processing. If the channel is full or the server restarts, a background database sweeper picks up pending jobs from PostgreSQL.
- 🧬 **Embedded JavaScript Transformations**: Evaluates user-defined ECMAScript snippets to transform incoming JSON payloads before forwarding. Implemented using `goja`. Includes two key optimizations:
  - 🧠 **AST Caching**: Scripts are compiled to machine bytecode (`*goja.Program`) once and cached in a `sync.Map`.
  - 🏊‍♂️ **VM Pooling**: `goja.Runtime` instances are pooled using a `sync.Pool` to avoid memory allocation overhead during concurrent requests.
- 🛡️ **Reliability & Backoff**: Failed webhook deliveries are retried using exponential backoff with "Full Jitter" to prevent thundering herd scenarios on target servers.
- 📡 **Real-Time Observability**: A WebSocket hub broadcasts captured webhooks (`models.Request`) in real-time to connected clients for specific endpoints.

## 🛠️ Tech Stack
*   **Language**: Go 1.26
*   **Database**: PostgreSQL (via `pgxpool` and `golang-migrate`)
*   **Router**: `go-chi/chi`
*   **JS Engine**: `github.com/dop251/goja`
*   **WebSockets**: `gorilla/websocket`

## 🔌 API Endpoints

### Public
*   `POST /hooks/{id}` - Capture a webhook payload and push to the processing queue.

### Protected (Requires `X-API-Key` header)
*   `POST /endpoints` - Create a new endpoint (Accepts `name`, `target_url`, and optional `transformer_script`).
*   `GET /endpoints` - List all configured endpoints.
*   `GET /endpoints/{id}` - Get details for a specific endpoint.
*   `DELETE /endpoints/{id}` - Delete an endpoint and its cascading data.
*   `GET /endpoints/{id}/requests` - View historical webhook requests.
*   `POST /endpoints/{id}/replay/{request_id}` - Enqueue a specific request for replay.
*   `GET /ws/{id}` - WebSocket upgrade for real-time endpoint logs.

## 🚀 Getting Started

1. Ensure PostgreSQL is running.
2. Configure your environment variables (Database URL, Port, API Key) in `.env` (or let `config.Load()` use defaults).
3. Run the server (migrations will apply automatically):
```bash
go run cmd/server/main.go
```
