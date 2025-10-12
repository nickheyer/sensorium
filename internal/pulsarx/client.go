package pulsarx

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

func New(url string) (pulsar.Client, error) {
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		OperationTimeout:  0, // use defaults
		ConnectionTimeout: 0,
	})
}
