package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"sensorium/internal/config"
	"sensorium/internal/pulsarx"
	"sensorium/internal/roles/agent"
	"sensorium/internal/roles/alerter"
	"sensorium/internal/roles/detector"
	"sensorium/internal/roles/ui"
)

func main() {
	// Load configs
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Sensorium with roles: %v", cfg.Roles)
	log.Printf("Connecting to Pulsar at: %s", cfg.PulsarURL)
	if cfg.TLS.Enabled {
		log.Printf("TLS enabled")
		if cfg.TLS.CertFilePath != "" {
			log.Printf("Using mTLS with client cert")
		}
	}

	// Create Pulsar client
	client, err := pulsarx.New(cfg.PulsarURL, &cfg.TLS)
	if err != nil {
		log.Fatalf("Failed to connect to Pulsar: %v", err)
	}
	defer client.Close()

	log.Println("Successfully connected to Pulsar")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Go(func() {
			log.Printf("Starting role: %s", name)
			retryCount := 0
			for {
				if err := fn(ctx); err != nil {
					if err == context.Canceled {
						log.Printf("[%s] Context canceled, shutting down", name)
						return
					}
					retryCount++
					log.Printf("[%s] Error (attempt %d): %v - retrying in 2s", name, retryCount, err)
					select {
					case <-time.After(2 * time.Second):
						continue
					case <-ctx.Done():
						return
					}
				}
				log.Printf("[%s] Completed successfully", name)
				return
			}
		})
	}

	// Start roles
	for _, role := range cfg.Roles {
		switch role {
		case "agent":
			start("agent", func(ctx context.Context) error {
				return agent.Run(ctx, client, cfg.Agent)
			})
		case "detector":
			start("detector", func(ctx context.Context) error {
				return detector.Run(ctx, client, cfg.Detector)
			})
		case "alerter":
			start("alerter", func(ctx context.Context) error {
				return alerter.Run(ctx, client, cfg.Alerter)
			})
		case "ui":
			start("ui", func(ctx context.Context) error {
				return ui.Run(ctx, client, cfg)
			})
		default:
			log.Fatalf("Unknown role: %q", role)
		}
	}

	// Wait for int signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	log.Println("All roles started. Press Ctrl+C to shutdown.")

	<-sigCh
	log.Println("Shutdown signal received, stopping all roles...")

	// Cancel out and wait for goroutines
	cancel()
	wg.Wait()

	log.Println("All roles stopped. Goodbye!")
}
