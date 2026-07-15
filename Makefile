.PHONY: run build test clean migrate-new

# Variables
APP_NAME=vahak
MAIN_PATH=cmd/server/main.go
MIGRATIONS_PATH=migrations

run:
	@echo "🦅 Starting Vahak..."
	@go run $(MAIN_PATH)

build:
	@echo "🔨 Building Vahak..."
	@go build -o bin/$(APP_NAME) $(MAIN_PATH)

test:
	@echo "🧪 Running tests..."
	@go test -v ./...

clean:
	@echo "🧹 Cleaning up..."
	@rm -rf bin/

# Usage: make migrate-new name=add_users_table
migrate-new:
	@if [ -z "$(name)" ]; then \
		echo "Error: name is required. Usage: make migrate-new name=your_migration_name"; \
	else \
		migrate create -ext sql -dir $(MIGRATIONS_PATH) -seq $(name); \
		echo "✅ Migration created"; \
	fi

