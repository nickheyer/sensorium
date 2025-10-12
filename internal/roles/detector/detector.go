package detector

import (
	"context"
	"fmt"
	"log"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

func Run(ctx context.Context, client pulsar.Client, cfg *config.Config) error {
	log.Printf("Detector starting - In: %s, Out: %s", cfg.Detector.InTopic, cfg.Detector.OutTopic)

	cons, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.Detector.InTopic,
		SubscriptionName: cfg.Detector.SubName,
		Type:             pulsar.KeyShared,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries:   5,
			DeadLetterTopic: cfg.Detector.InTopic + ".DLQ",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer cons.Close()

	log.Printf("Detector subscribed to %s", cfg.Detector.InTopic)

	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.Detector.OutTopic,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer prod.Close()

	log.Printf("Detector producer created for %s", cfg.Detector.OutTopic)

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
		if mf.CpuUsagePct >= cfg.Detector.CPUWarn {
			sev := "warn"
			if mf.CpuUsagePct >= cfg.Detector.CPUCrit {
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
			if ratio >= cfg.Detector.MemWarn {
				sev := "warn"
				if ratio >= cfg.Detector.MemCrit {
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
	log.Printf("Detector emitted event: %s/%s/%s, MsgID: %v", ev.NodeId, ev.Kind, ev.Severity, msgID)
}
