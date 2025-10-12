package ui

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

func Run(ctx context.Context, client pulsar.Client, cfg *config.Config) error {
	log.Printf("UI starting on %s", cfg.UI.HTTPAddr)

	// Subscribe alerts
	alertSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.UI.AlertsTopic,
		SubscriptionName: cfg.UI.SubPrefix + "-ui-alerts",
		Type:             pulsar.Shared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to alerts: %w", err)
	}
	defer alertSub.Close()

	log.Printf("UI subscribed to alerts topic: %s", cfg.UI.AlertsTopic)

	// Subscribe metrics (latest per node)
	metricSub, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.UI.MetricsTopic,
		SubscriptionName: cfg.UI.SubPrefix + "-ui-metrics",
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to metrics: %w", err)
	}
	defer metricSub.Close()

	log.Printf("UI subscribed to metrics topic: %s", cfg.UI.MetricsTopic)

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

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		fmt.Fprintln(w, strings.Repeat("=", 100))
		fmt.Fprintf(w, " SENSORIUM MONITORING SYSTEM - %s\n", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Fprintln(w, strings.Repeat("=", 100))

		// Sort nodes
		var nodeIDs []string
		for node := range latestByNode {
			nodeIDs = append(nodeIDs, node)
		}
		sort.Strings(nodeIDs)

		for _, nodeID := range nodeIDs {
			mf := latestByNode[nodeID]
			age := time.Since(time.UnixMilli(mf.TsMs))
			next := cfg.Agent.SamplePeriod - age

			// Node header
			fmt.Fprintln(w)
			fmt.Fprintf(w, " NODE: %s", nodeID)
			if mf.Hostname != "" && mf.Hostname != nodeID {
				fmt.Fprintf(w, " (%s)", mf.Hostname)
			}
			fmt.Fprintf(w, " - Last Update: %s ago", age.Truncate(time.Second))
			fmt.Fprintf(w, " - Next Update: in %s\n", next.Truncate(time.Second))
			fmt.Fprintln(w, strings.Repeat("-", 100))

			// Sys info
			if mf.OsPlatform != "" || mf.KernelVersion != "" {
				fmt.Fprintf(w, " System: %s %s | Kernel: %s %s | Uptime: %s\n",
					mf.OsPlatform, mf.OsVersion, mf.KernelVersion, mf.KernelArch,
					formatDuration(time.Duration(mf.UptimeSeconds)*time.Second))
			}

			// CPU
			fmt.Fprintln(w, "\n CPU:")
			if mf.CpuModel != "" {
				fmt.Fprintf(w, "  Model: %s\n", mf.CpuModel)
			}
			fmt.Fprintf(w, "  Cores: %d physical / %d logical\n", mf.CpuCountPhysical, mf.CpuCountLogical)
			fmt.Fprintf(w, "  Usage: %.1f%% overall", mf.CpuUsagePct)
			if mf.CpuTempC > 0 {
				fmt.Fprintf(w, " | Temp: %.1f°C", mf.CpuTempC)
			}
			fmt.Fprintln(w)

			// CPU per-core
			if len(mf.CpuCorePcts) > 0 {
				fmt.Fprint(w, "  Per-Core: ")
				for i, pct := range mf.CpuCorePcts {
					if i > 0 {
						fmt.Fprint(w, ", ")
					}
					fmt.Fprintf(w, "C%d: %.0f%%", i, pct)
				}
				fmt.Fprintln(w)
			}

			// Load avgs
			if mf.LoadAvg_1M > 0 || mf.LoadAvg_5M > 0 || mf.LoadAvg_15M > 0 {
				fmt.Fprintf(w, "  Load Avg: %.2f (1m) | %.2f (5m) | %.2f (15m)\n",
					mf.LoadAvg_1M, mf.LoadAvg_5M, mf.LoadAvg_15M)
			}

			// Process/thread counts
			fmt.Fprintf(w, "  Processes: %d | Threads: %d\n", mf.ProcessCount, mf.ThreadCount)

			// Memory
			fmt.Fprintln(w, "\n Memory:")
			memUsedPct := float32(0)
			if mf.MemTotal > 0 {
				memUsedPct = float32(mf.MemUsed) / float32(mf.MemTotal) * 100
			}
			fmt.Fprintf(w, "  RAM: %s / %s (%.1f%%) | Available: %s | Free: %s\n",
				formatBytes(mf.MemUsed), formatBytes(mf.MemTotal), memUsedPct,
				formatBytes(mf.MemAvailable), formatBytes(mf.MemFree))

			if mf.MemCached > 0 || mf.MemBuffers > 0 {
				fmt.Fprintf(w, "  Cache: %s | Buffers: %s\n",
					formatBytes(mf.MemCached), formatBytes(mf.MemBuffers))
			}

			if mf.SwapTotal > 0 {
				swapUsedPct := float32(0)
				if mf.SwapTotal > 0 {
					swapUsedPct = float32(mf.SwapUsed) / float32(mf.SwapTotal) * 100
				}
				fmt.Fprintf(w, "  Swap: %s / %s (%.1f%%)\n",
					formatBytes(mf.SwapUsed), formatBytes(mf.SwapTotal), swapUsedPct)
			}

			// Disk
			if len(mf.DiskUsages) > 0 {
				fmt.Fprintln(w, "\n Disk Usage:")
				for _, disk := range mf.DiskUsages {
					fmt.Fprintf(w, "  %s (%s): %s / %s (%.1f%%) - %s\n",
						disk.MountPoint, disk.FsType,
						formatBytes(disk.Used), formatBytes(disk.Total), disk.UsagePct,
						disk.Device)
				}
			}

			// Disk IO
			if len(mf.DiskIos) > 0 {
				fmt.Fprintln(w, "\n Disk I/O:")
				for _, io := range mf.DiskIos {
					fmt.Fprintf(w, "  %s: R: %s (%d ops) | W: %s (%d ops)\n",
						io.Device,
						formatBytes(io.ReadBytes), io.ReadCount,
						formatBytes(io.WriteBytes), io.WriteCount)
				}
			}

			// Net
			if len(mf.NetInterfaces) > 0 {
				fmt.Fprintln(w, "\n Network Interfaces:")
				for _, net := range mf.NetInterfaces {
					fmt.Fprintf(w, "  %s: TX: %s (%d pkts) | RX: %s (%d pkts)",
						net.Name,
						formatBytes(net.BytesSent), net.PacketsSent,
						formatBytes(net.BytesRecv), net.PacketsRecv)
					if net.ErrIn > 0 || net.ErrOut > 0 || net.DropIn > 0 || net.DropOut > 0 {
						fmt.Fprintf(w, " | Errs: %d/%d | Drops: %d/%d",
							net.ErrIn, net.ErrOut, net.DropIn, net.DropOut)
					}
					fmt.Fprintln(w)
				}
			}

			// Connections
			if mf.ConnEstablished > 0 || mf.ConnListen > 0 {
				fmt.Fprintln(w, "\n Network Connections:")
				fmt.Fprintf(w, "  Established: %d | Listen: %d | Time Wait: %d | Close Wait: %d\n",
					mf.ConnEstablished, mf.ConnListen, mf.ConnTimeWait, mf.ConnCloseWait)
			}

			// Temps
			if len(mf.TempSensors) > 0 {
				fmt.Fprintln(w, "\n Temperature Sensors:")
				for _, sensor := range mf.TempSensors {
					fmt.Fprintf(w, "  %s: %.1f°C", sensor.Name, sensor.TemperatureC)
					if sensor.HighC > 0 || sensor.CriticalC > 0 {
						fmt.Fprintf(w, " (high: %.1f°C, critical: %.1f°C)", sensor.HighC, sensor.CriticalC)
					}
					fmt.Fprintln(w)
				}
			}

			// Top processes by CPU
			if len(mf.TopCpuProcs) > 0 {
				fmt.Fprintln(w, "\n Top Processes by CPU:")
				for _, proc := range mf.TopCpuProcs {
					fmt.Fprintf(w, "  PID %d: %s (%.1f%% CPU, %.1f%% MEM) - %s\n",
						proc.Pid, proc.Name, proc.CpuPct, proc.MemPct, proc.Username)
				}
			}

			// Top processes by Memory
			if len(mf.TopMemProcs) > 0 {
				fmt.Fprintln(w, "\n Top Processes by Memory:")
				for _, proc := range mf.TopMemProcs {
					fmt.Fprintf(w, "  PID %d: %s (%.1f%% MEM, %s RSS) - %s\n",
						proc.Pid, proc.Name, proc.MemPct, formatBytes(proc.MemRss), proc.Username)
				}
			}
		}

		// Alerts
		fmt.Fprintln(w)
		fmt.Fprintln(w, strings.Repeat("=", 100))
		fmt.Fprintf(w, " RECENT ALERTS (Last %d)\n", len(recentAlerts))
		fmt.Fprintln(w, strings.Repeat("=", 100))

		if len(recentAlerts) > 0 {
			for i := len(recentAlerts) - 1; i >= 0; i-- {
				al := recentAlerts[i]
				ev := al.Event
				alertTime := time.UnixMilli(ev.TsMs)
				fmt.Fprintf(w, " [%s] %s | Node: %s | Type: %s | Severity: %s\n",
					alertTime.Format("15:04:05"),
					al.Id, ev.NodeId, ev.Kind, ev.Severity)
				if ev.Detail != "" {
					fmt.Fprintf(w, "   Details: %s\n", ev.Detail)
				}
			}
		} else {
			fmt.Fprintln(w, " No recent alerts")
		}

		fmt.Fprintln(w)
		fmt.Fprintln(w, strings.Repeat("=", 100))
	})

	srv := &http.Server{Addr: cfg.UI.HTTPAddr}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("UI HTTP server listening on %s", cfg.UI.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}

// Helpers

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	} else {
		return fmt.Sprintf("%dm", minutes)
	}
}
