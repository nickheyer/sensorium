package alerter

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
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
	Cooldown time.Duration
}

func Run(ctx context.Context, client pulsar.Client, cfg Config) error {
	cons, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic: cfg.InTopic, SubscriptionName: cfg.SubName, Type: pulsar.Shared,
	})
	if err != nil {
		return err
	}
	defer cons.Close()

	prod, err := client.CreateProducer(pulsar.ProducerOptions{Topic: cfg.OutTopic})
	if err != nil {
		return err
	}
	defer prod.Close()

	last := map[string]time.Time{} // key: node.kind -> last sent

	for {
		msg, err := cons.Receive(ctx)
		if err != nil {
			return err
		}
		var ev sensorpb.HealthEvent
		if err := proto.Unmarshal(msg.Payload(), &ev); err != nil {
			cons.Nack(msg)
			continue
		}
		k := ev.NodeId + "|" + ev.Kind
		now := time.Now()
		if t, ok := last[k]; ok && now.Sub(t) < cfg.Cooldown {
			cons.Ack(msg)
			continue
		}
		last[k] = now

		al := &sensorpb.Alert{
			Id:    hash(k + "|" + ev.Severity),
			Event: &ev,
		}
		b, _ := proto.Marshal(al)
		msgID, _ := prod.Send(ctx, &pulsar.ProducerMessage{
			Key: ev.NodeId, Payload: b,
		})
		cons.Ack(msg)
		fmt.Printf("Pulsar alerter sent producer-message w/ ID: %d\n", msgID)
	}
}

func hash(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}
