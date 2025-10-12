package detector

import (
	"context"
	"fmt"
	"time"

	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	SubName  string
	InTopic  string
	OutTopic string
	CPUWarn  float32 // e.g., 85
	CPUCrit  float32 // e.g., 92
	MemWarn  float32 // e.g., 0.90 (90% of total)
	MemCrit  float32 // e.g., 0.95
}

func Run(ctx context.Context, client pulsar.Client, cfg Config) error {
	cons, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.InTopic,
		SubscriptionName: cfg.SubName,
		Type:             pulsar.KeyShared,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries:   5,
			DeadLetterTopic: cfg.InTopic + ".DLQ",
		},
	})
	if err != nil {
		return err
	}
	defer cons.Close()

	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.OutTopic,
	})
	if err != nil {
		return err
	}
	defer prod.Close()

	for {
		msg, err := cons.Receive(ctx)
		if err != nil {
			return err
		}
		var mf sensorpb.MetricFrame
		if err := proto.Unmarshal(msg.Payload(), &mf); err != nil {
			cons.Nack(msg)
			continue
		}

		// CPU check
		if mf.CpuUsagePct >= cfg.CPUWarn {
			sev := "warn"
			if mf.CpuUsagePct >= cfg.CPUCrit {
				sev = "crit"
			}
			ev := &sensorpb.HealthEvent{
				NodeId:   mf.NodeId,
				TsMs:     time.Now().UnixMilli(),
				Kind:     "cpu_hot",
				Severity: sev,
				Detail:   fmt.Sprintf("cpu=%.1f%%", mf.CpuUsagePct),
			}
			emit(ctx, prod, mf.NodeId, ev)
		}

		// Memory check
		if mf.MemTotal > 0 {
			ratio := float32(mf.MemUsed) / float32(mf.MemTotal)
			if ratio >= cfg.MemWarn {
				sev := "warn"
				if ratio >= cfg.MemCrit {
					sev = "crit"
				}
				ev := &sensorpb.HealthEvent{
					NodeId:   mf.NodeId,
					TsMs:     time.Now().UnixMilli(),
					Kind:     "mem_high",
					Severity: sev,
					Detail:   fmt.Sprintf("mem=%.1f%%", ratio*100),
				}
				emit(ctx, prod, mf.NodeId, ev)
			}
		}

		cons.Ack(msg)
	}
}

func emit(ctx context.Context, prod pulsar.Producer, key string, ev *sensorpb.HealthEvent) {
	b, _ := proto.Marshal(ev)
	msgID, _ := prod.Send(ctx, &pulsar.ProducerMessage{
		Key: key, Payload: b,
	})
	fmt.Printf("Pulsar detector sent producer-message w/ ID: %d\n", msgID)
}
