MODULE := dash0.com/otlp-log-processor-backend

.PHONY: build run test test-integration test-all fmt vet lint tidy clean

build:
	go build ./...

run:
	LOG_LEVEL=debug go run .

test:
	LOG_LEVEL=debug go test -count=1 ./...

test-integration:
	LOG_LEVEL=debug OTEL_DEBUG=1 go test -tags integration -count=1 -v ./...

test-all: test test-integration

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: vet
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy:
	go mod tidy

clean:
	go clean ./...
