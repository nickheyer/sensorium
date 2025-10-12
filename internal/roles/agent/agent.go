package agent

import (
	"context"
	"fmt"
	"log"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"google.golang.org/protobuf/proto"
)

func Run(ctx context.Context, client pulsar.Client, cfg config.AgentConfig) error {
	log.Printf("Agent starting with NodeID: %s, Topic: %s", cfg.NodeID, cfg.Topic)

	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           cfg.Topic,
		DisableBatching: false,
		CompressionType: pulsar.LZ4,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer prod.Close()

	log.Printf("Agent producer created successfully")

	t := time.NewTicker(cfg.SamplePeriod)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			var cpuPct float64
			if pcts, _ := cpu.Percent(0, false); len(pcts) > 0 {
				cpuPct = pcts[0]
			}
			vm, _ := mem.VirtualMemory()

			m := &sensorpb.MetricFrame{
				NodeId:      cfg.NodeID,
				TsMs:        time.Now().UnixMilli(),
				CpuUsagePct: float32(cpuPct),
				CpuTempC:    0, // left for later (IPMI/ACPI integration)
			}
			if vm != nil {
				m.MemUsed = vm.Used
				m.MemTotal = vm.Total
			}
			b, _ := proto.Marshal(m)
			msgID, _ := prod.Send(ctx, &pulsar.ProducerMessage{
				Key:     cfg.NodeID,
				Payload: b,
			})

			log.Printf("Agent sent metrics - CPU: %.1f%%, Mem: %d/%d, MsgID: %v",
				cpuPct, m.MemUsed, m.MemTotal, msgID)
		}
	}
}
