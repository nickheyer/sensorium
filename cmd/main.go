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
	// Load configuration from file, env, and flags
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Sensorium with roles: %v", cfg.Roles)
	log.Printf("Connecting to Pulsar at: %s", cfg.PulsarURL)

	// Create Pulsar client with better timeout settings
	client, err := pulsarx.New(cfg.PulsarURL)
	if err != nil {
		log.Fatalf("Failed to connect to Pulsar: %v", err)
	}
	defer client.Close()

	log.Println("Successfully connected to Pulsar")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	// Start roles based on configuration
	for _, role := range cfg.Roles {
		switch role {
		case "agent":
			start("agent", func(ctx context.Context) error {
				return agent.Run(ctx, client, cfg)
			})
		case "detector":
			start("detector", func(ctx context.Context) error {
				return detector.Run(ctx, client, cfg)
			})
		case "alerter":
			start("alerter", func(ctx context.Context) error {
				return alerter.Run(ctx, client, cfg)
			})
		case "ui":
			start("ui", func(ctx context.Context) error {
				return ui.Run(ctx, client, cfg)
			})
		default:
			log.Fatalf("Unknown role: %q", role)
		}
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	log.Println("All roles started. Press Ctrl+C to shutdown.")

	<-sigCh
	log.Println("Shutdown signal received, stopping all roles...")

	// Cancel context and wait for goroutines
	cancel()
	wg.Wait()

	log.Println("All roles stopped. Goodbye!")
}