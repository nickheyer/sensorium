APP := sensorium

.PHONY: all proto build run clean up down

all: build

proto:
	@protoc --go_out=. proto/sensorium.proto

build: proto
	@go build -o bin/$(APP) ./cmd

run-pulsar:
	@docker compose up -d pulsar

down:
	@docker compose down -v

run-ui:
	@SENSORIUM_ROLES=ui PULSAR_URL=pulsar://localhost:6650 go run ./cmd

run-detector:
	@SENSORIUM_ROLES=detector PULSAR_URL=pulsar://localhost:6650 go run ./cmd

run-alerter:
	@SENSORIUM_ROLES=alerter PULSAR_URL=pulsar://localhost:6650 go run ./cmd

run-agent:
	@SENSORIUM_ROLES=agent NODE_ID=$$(hostname) SAMPLE_MS=2000 PULSAR_URL=pulsar://localhost:6650 go run ./cmd

clean:
	@rm -rf bin
