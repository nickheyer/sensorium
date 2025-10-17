package pulsarx

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strings"
	"time"

	"sensorium/internal/config"
	"sensorium/internal/pki"

	"github.com/apache/pulsar-client-go/pulsar"
)

func New(url string, tlsConfig *config.TLSConfig) (pulsar.Client, error) {
	log.Printf("Creating Pulsar client for: %s", url)

	clientOptions := pulsar.ClientOptions{
		URL:                     url,
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       10 * time.Second,
		MaxConnectionsPerBroker: 10,
		EnableTransaction:       false,
	}

	if tlsConfig != nil && tlsConfig.Enabled {
		log.Printf("Configuring TLS for Pulsar client")

		// URL scheme update
		if after, ok := strings.CutPrefix(url, "pulsar://"); ok {
			url = "pulsar+ssl://" + after
			clientOptions.URL = url
			log.Printf("Updated URL to use TLS: %s", url)
		}

		// Use embedded certs unless provided
		if tlsConfig.TrustCertsFilePath == "" && len(pki.CACert) > 0 {
			log.Printf("Using embedded certificates for mTLS from binary")
			customTLS := &tls.Config{
				InsecureSkipVerify: tlsConfig.AllowInsecureConn,
			}

			// Load CA cert from embedded
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(pki.CACert) {
				return nil, fmt.Errorf("failed to parse embedded CA certificate")
			}
			customTLS.RootCAs = caCertPool

			// Load client cert/key from embedded ~ mTLS
			clientCert, err := tls.X509KeyPair(pki.ClientCert, pki.ClientKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load embedded client certificate: %w", err)
			}
			customTLS.Certificates = []tls.Certificate{clientCert}
			clientOptions.TLSConfig = customTLS
			clientOptions.TLSValidateHostname = tlsConfig.ValidateHostname

			// Also set up TLS authentication using the embedded certificates
			// This is required for Pulsar to use the certificate for authentication, not just transport
			clientOptions.Authentication = pulsar.NewAuthenticationFromTLSCertSupplier(func() (*tls.Certificate, error) {
				cert, err := tls.X509KeyPair(pki.ClientCert, pki.ClientKey)
				if err != nil {
					return nil, err
				}
				return &cert, nil
			})
		} else {
			// Use certs from file
			clientOptions.TLSTrustCertsFilePath = tlsConfig.TrustCertsFilePath
			clientOptions.TLSAllowInsecureConnection = tlsConfig.AllowInsecureConn
			clientOptions.TLSValidateHostname = tlsConfig.ValidateHostname

			// mTLS
			if tlsConfig.CertFilePath != "" && tlsConfig.KeyFilePath != "" {
				log.Printf("Configuring mTLS with client certificate from files")
				clientOptions.Authentication = pulsar.NewAuthenticationTLS(
					tlsConfig.CertFilePath,
					tlsConfig.KeyFilePath,
				)
			} else if tlsConfig.CertFilePath != "" || tlsConfig.KeyFilePath != "" {
				return nil, fmt.Errorf("both cert_file and key_file must be provided for mTLS")
			}
		}
	}

	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}

	log.Printf("Pulsar client created successfully")
	return client, nil
}
