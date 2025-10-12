package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"sensorium/internal/pulsarx"
	"sensorium/internal/roles/agent"
	"sensorium/internal/roles/alerter"
	"sensorium/internal/roles/detector"
	"sensorium/internal/roles/ui"
)

func main() {
	var (
		roles     = env("SENSORIUM_ROLES", "ui")
		pulsarURL = env("PULSAR_URL", "pulsar://localhost:6650")
		subPrefix = env("SUB_PREFIX", "sensorium")
		httpAddr  = env("HTTP_ADDR", ":8088")
		nodeID    = env("NODE_ID", hostnameOr("node-1"))
		sampleMS  = intFlag("SAMPLE_MS", 2000)
		cooldown  = envDur("ALERT_COOLDOWN", 10*time.Minute)
	)
	flag.Parse()

	client, err := pulsarx.New(pulsarURL)
	if err != nil {
		log.Fatalf("pulsar: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := fn(ctx); err != nil {
					log.Printf("[%s] exited: %v (retrying in 2s)", name, err)
					select {
					case <-time.After(2 * time.Second):
						continue
					case <-ctx.Done():
						return
					}
				}
				return
			}
		}()
	}

	for _, r := range strings.Split(roles, ",") {
		switch strings.TrimSpace(r) {
		case "agent":
			start("agent", func(ctx context.Context) error {
				return agent.Run(ctx, client, agent.Config{
					NodeID: nodeID,
					Topic:  "sensor.node.metrics",
					Sample: time.Duration(sampleMS) * time.Millisecond,
				})
			})
		case "detector":
			start("detector", func(ctx context.Context) error {
				return detector.Run(ctx, client, detector.Config{
					SubName:  subPrefix + "-detector",
					InTopic:  "sensor.node.metrics",
					OutTopic: "sensor.node.health",
					CPUWarn:  85, CPUCrit: 92,
					MemWarn: 0.90, MemCrit: 0.95,
				})
			})
		case "alerter":
			start("alerter", func(ctx context.Context) error {
				return alerter.Run(ctx, client, alerter.Config{
					SubName:  subPrefix + "-alerter",
					InTopic:  "sensor.node.health",
					OutTopic: "sensor.alerts",
					Cooldown: cooldown,
				})
			})
		case "ui":
			start("ui", func(ctx context.Context) error {
				return ui.Run(ctx, client, ui.Config{
					Addr:         httpAddr,
					AlertsTopic:  "sensor.alerts",
					MetricsTopic: "sensor.node.metrics",
					SubPrefix:    subPrefix,
				})
			})
		default:
			log.Fatalf("unknown role: %q", r)
		}
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	// Cancel context and wait for goroutines
	cancel()
	wg.Wait()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
func intFlag(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}
func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
