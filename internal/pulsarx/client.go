package pulsarx

import (
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func New(url string) (pulsar.Client, error) {
	log.Printf("Creating Pulsar client for: %s", url)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     url,
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       10 * time.Second,
		MaxConnectionsPerBroker: 10,
		EnableTransaction:       false,
	})

	if err != nil {
		return nil, err
	}

	log.Printf("Pulsar client created successfully")
	return client, nil
}
