package alerter

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
)

func Run(ctx context.Context, client pulsar.Client, cfg *config.Config) error {
	log.Printf("Alerter starting - In: %s, Out: %s, Cooldown: %v",
		cfg.Alerter.InTopic, cfg.Alerter.OutTopic, cfg.Alerter.Cooldown)

	cons, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.Alerter.InTopic,
		SubscriptionName: cfg.Alerter.SubName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer cons.Close()

	log.Printf("Alerter subscribed to %s", cfg.Alerter.InTopic)

	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.Alerter.OutTopic,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer prod.Close()

	log.Printf("Alerter producer created for %s", cfg.Alerter.OutTopic)

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
		if t, ok := last[k]; ok && now.Sub(t) < cfg.Alerter.Cooldown {
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
		log.Printf("Alerter emitted alert: %s for %s/%s, MsgID: %v", al.Id, ev.NodeId, ev.Kind, msgID)
	}
}

func hash(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}
