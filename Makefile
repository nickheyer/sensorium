APP := sensorium

# ENV
export GOBIN := $(shell pwd)/bin
export GOPATH := $(shell go env GOPATH)
export PATH := $(GOBIN):$(PATH)

.PHONY: all gen build dev pulsar-up pulsar-down clean deps certs dev-tls

all: build

deps:
	@go mod tidy
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

gen:
	@protoc --go_out=. proto/sensorium.proto

certs: clean
	@mkdir -p internal/pki/embedded
	@if [ ! -f internal/pki/embedded/ca.crt ]; then \
		echo "Generating embedded certificates..."; \
		openssl genrsa -out internal/pki/embedded/ca.key 2048 2>/dev/null; \
		openssl req -new -x509 -days 3650 -key internal/pki/embedded/ca.key -out internal/pki/embedded/ca.crt \
			-subj "/C=US/O=Sensorium/CN=Sensorium-CA" 2>/dev/null; \
		openssl genrsa -out internal/pki/embedded/client.key 2048 2>/dev/null; \
		openssl pkcs8 -topk8 -nocrypt -in internal/pki/embedded/client.key -out internal/pki/embedded/client-pk8.key 2>/dev/null; \
		openssl req -new -key internal/pki/embedded/client.key -out internal/pki/embedded/client.csr \
			-subj "/C=US/O=Sensorium/CN=sensorium-client" 2>/dev/null; \
		openssl x509 -req -days 3650 -in internal/pki/embedded/client.csr -CA internal/pki/embedded/ca.crt \
			-CAkey internal/pki/embedded/ca.key -out internal/pki/embedded/client.crt -CAcreateserial 2>/dev/null; \
		openssl genrsa -out internal/pki/embedded/broker.key 2048 2>/dev/null; \
		openssl pkcs8 -topk8 -nocrypt -in internal/pki/embedded/broker.key -out internal/pki/embedded/broker-pk8.key 2>/dev/null; \
		echo "subjectAltName=DNS:*,DNS:*.local,DNS:*.localdomain,DNS:localhost,DNS:pulsar" > internal/pki/embedded/broker.ext; \
		openssl req -new -key internal/pki/embedded/broker.key -out internal/pki/embedded/broker.csr \
			-subj "/C=US/O=Sensorium/CN=localhost" 2>/dev/null; \
		openssl x509 -req -days 3650 -in internal/pki/embedded/broker.csr -CA internal/pki/embedded/ca.crt \
			-CAkey internal/pki/embedded/ca.key -out internal/pki/embedded/broker.crt -CAcreateserial \
			-extfile internal/pki/embedded/broker.ext 2>/dev/null; \
		rm -f internal/pki/embedded/*.csr internal/pki/embedded/*.srl internal/pki/embedded/*.ext; \
		chmod 644 internal/pki/embedded/*.key; \
		echo "Certificates generated in internal/pki/embedded/"; \
	fi

build: clean gen certs
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

dev-tls:
	@bash -c 'trap "echo \"\\nShutting down Pulsar...\"; docker compose -f docker-compose.pulsar-tls.yml down -v" EXIT INT TERM; \
		docker compose -f docker-compose.pulsar-tls.yml up -d && \
		sleep 3 && \
		SENSORIUM_ROLES=agent,detector,alerter,ui \
		SENSORIUM_TLS_ENABLED=true \
		SENSORIUM_PULSAR_URL=pulsar+ssl://localhost:6651 \
		SENSORIUM_NODE_ID=$$(hostname) \
		SENSORIUM_SAMPLE_PERIOD=2s \
		SENSORIUM_HTTP_ADDR=:8088 \
		SENSORIUM_HTTPS_ADDR=:8443 \
		go run ./cmd'

pulsar-up:
	@docker compose -f docker-compose.pulsar.yml up -d

pulsar-down:
	@docker compose -f docker-compose.pulsar.yml down -v

clean:
	@rm -rf bin
	@rm -rf internal/pki/embedded

run-ui:
	@SENSORIUM_ROLES=ui go run ./cmd

run-agent:
	@SENSORIUM_ROLES=agent go run ./cmd

run-detector:
	@SENSORIUM_ROLES=detector go run ./cmd

run-alerter:
	@SENSORIUM_ROLES=alerter go run ./cmd