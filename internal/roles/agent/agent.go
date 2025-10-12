package agent

import (
	"context"
	"fmt"
	"time"

	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	NodeID string
	Topic  string
	Sample time.Duration
}

func Run(ctx context.Context, client pulsar.Client, cfg Config) error {
	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           cfg.Topic,
		DisableBatching: false,
		CompressionType: pulsar.LZ4,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	t := time.NewTicker(cfg.Sample)
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

			fmt.Printf("Pulsar agent sent producer-message w/ ID: %d\n", msgID)
		}
	}
}
