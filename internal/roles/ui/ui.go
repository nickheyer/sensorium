package ui

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Addr         string
	AlertsTopic  string
	MetricsTopic string // optional: subscribe to show latest per node
	SubPrefix    string
}

func Run(ctx context.Context, client pulsar.Client, cfg Config) error {
	// Subscribe alerts
	alertSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.AlertsTopic,
		SubscriptionName: cfg.SubPrefix + "-ui-alerts",
		Type:             pulsar.Shared,
	})
	if err != nil {
		return err
	}

	// Subscribe metrics (latest per node)
	metricSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.MetricsTopic,
		SubscriptionName: cfg.SubPrefix + "-ui-metrics",
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return err
	}

	var (
		mu           sync.RWMutex
		latestByNode = map[string]*sensorpb.MetricFrame{}
		recentAlerts []*sensorpb.Alert
	)

	// Alerts consumer goroutine
	go func() {
		defer alertSub.Close()
		for {
			msg, err := alertSub.Receive(ctx)
			if err != nil {
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
			}
			alertSub.Ack(msg)
		}
	}()

	// Metrics consumer goroutine
	go func() {
		defer metricSub.Close()
		for {
			msg, err := metricSub.Receive(ctx)
			if err != nil {
				return
			}
			var mf sensorpb.MetricFrame
			if err := proto.Unmarshal(msg.Payload(), &mf); err == nil {
				mu.Lock()
				defer mu.Unlock()
				latestByNode[mf.NodeId] = &mf
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

		fmt.Fprintf(w, "SENSORIUM UI (plaintext)\n\n")
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

	srv := &http.Server{Addr: cfg.Addr}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	return srv.ListenAndServe()
}
