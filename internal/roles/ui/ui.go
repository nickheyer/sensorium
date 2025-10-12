package ui

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

func Run(ctx context.Context, client pulsar.Client, cfg config.UIConfig) error {
	log.Printf("UI starting on %s", cfg.HTTPAddr)

	// Subscribe alerts
	alertSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.AlertsTopic,
		SubscriptionName: cfg.SubPrefix + "-ui-alerts",
		Type:             pulsar.Shared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to alerts: %w", err)
	}
	defer alertSub.Close()

	log.Printf("UI subscribed to alerts topic: %s", cfg.AlertsTopic)

	// Subscribe metrics (latest per node)
	metricSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.MetricsTopic,
		SubscriptionName: cfg.SubPrefix + "-ui-metrics",
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to metrics: %w", err)
	}
	defer metricSub.Close()

	log.Printf("UI subscribed to metrics topic: %s", cfg.MetricsTopic)

	var (
		mu           sync.RWMutex
		latestByNode = map[string]*sensorpb.MetricFrame{}
		recentAlerts []*sensorpb.Alert
	)

	// Alerts consumer goroutine
	go func() {
		for {
			msg, err := alertSub.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Context canceled
				}
				log.Printf("UI alerts consumer error: %v", err)
				return
			}
			var al sensorpb.Alert
			if err := proto.Unmarshal(msg.Payload(), &al); err == nil {
				mu.Lock()
				// keep last 50
				recentAlerts = append(recentAlerts, &al)
				if len(recentAlerts) > 50 {
					recentAlerts = recentAlerts[len(recentAlerts)-50:]
				}
				mu.Unlock()
				log.Printf("UI received alert: %s", al.Id)
			}
			alertSub.Ack(msg)
		}
	}()

	// Metrics consumer goroutine
	go func() {
		for {
			msg, err := metricSub.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Context canceled
				}
				log.Printf("UI metrics consumer error: %v", err)
				return
			}
			var mf sensorpb.MetricFrame
			if err := proto.Unmarshal(msg.Payload(), &mf); err == nil {
				mu.Lock()
				latestByNode[mf.NodeId] = &mf
				mu.Unlock()
			}
			metricSub.Ack(msg)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()

		type row struct {
			node string
			cpu  float32
			memP float32
			age  time.Duration
		}
		var rows []row
		for node, mf := range latestByNode {
			var memP float32
			if mf.MemTotal > 0 {
				memP = float32(mf.MemUsed) / float32(mf.MemTotal) * 100
			}
			age := time.Since(time.UnixMilli(mf.TsMs))
			rows = append(rows, row{node: node, cpu: mf.CpuUsagePct, memP: memP, age: age})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].node < rows[j].node })

		fmt.Fprintf(w, "SENSORIUM METRICS\n\n")
		fmt.Fprintf(w, "Nodes (%d):\n", len(rows))
		for _, r := range rows {
			fmt.Fprintf(w, "  - %s | CPU: %5.1f%% | MEM: %5.1f%% | last: %s\n",
				r.node, r.cpu, r.memP, r.age.Truncate(time.Second))
		}

		fmt.Fprintln(w, "\nRecent Alerts (up to 50):")
		for i := len(recentAlerts) - 1; i >= 0; i-- {
			al := recentAlerts[i]
			ev := al.Event
			fmt.Fprintf(w, "  - %s | %s | %s | %s | %s\n",
				al.Id, ev.NodeId, ev.Kind, ev.Severity, time.UnixMilli(ev.TsMs).Format(time.RFC3339))
		}
	})

	srv := &http.Server{Addr: cfg.HTTPAddr}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("UI HTTP server listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}
