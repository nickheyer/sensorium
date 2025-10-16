APP := sensorium

# ENV
export GOBIN := $(shell pwd)/bin
export GOPATH := $(shell go env GOPATH)
export PATH := $(GOBIN):$(PATH)

.PHONY: all gen build dev pulsar-up pulsar-down clean deps

all: build

deps:
	@go mod tidy
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

gen:
	@protoc --go_out=. proto/sensorium.proto

build: clean gen
	@go build -o bin/$(APP) ./cmd

dev:
	@bash -c 'trap "echo \"\\nShutting down Pulsar...\"; docker compose -f docker-compose.pulsar.yml down -v" EXIT INT TERM; \
		docker compose -f docker-compose.pulsar.yml up -d && \
		sleep 3 && \
		SENSORIUM_ROLES=agent,detector,alerter,ui \
		SENSORIUM_PULSAR_URL=pulsar://localhost:6650 \
		SENSORIUM_NODE_ID=$$(hostname) \
		SENSORIUM_SAMPLE_PERIOD=2s \
		SENSORIUM_HTTP_ADDR=:8088 \
		go run ./cmd'

pulsar-up:
	@docker compose -f docker-compose.pulsar.yml up -d

pulsar-down:
	@docker compose -f docker-compose.pulsar.yml down -v

clean:
	@rm -rf bin

run-ui:
	@SENSORIUM_ROLES=ui go run ./cmd

run-agent:
	@SENSORIUM_ROLES=agent go run ./cmd

run-detector:
	@SENSORIUM_ROLES=detector go run ./cmd

run-alerter:
	@SENSORIUM_ROLES=alerter go run ./cmd