APP := sensorium

.PHONY: all proto build dev pulsar-up pulsar-down clean

all: build

proto:
	@protoc --go_out=. proto/sensorium.proto

build: proto
	@go build -o bin/$(APP) ./cmd

dev: pulsar-up
	@sleep 3
	@SENSORIUM_ROLES=agent,detector,alerter,ui \
		PULSAR_URL=pulsar://localhost:6650 \
		NODE_ID=$$(hostname) \
		SAMPLE_PERIOD=2s \
		HTTP_ADDR=:8088 \
		go run ./cmd

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