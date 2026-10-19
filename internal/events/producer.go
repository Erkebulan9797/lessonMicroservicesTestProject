package events

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"debit-notifier/internal/model"
)

type Producer struct {
	w *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // один счёт - одна партиция, порядок сохраняется
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *Producer) Send(ctx context.Context, e model.DebitEvent) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.AccountID),
		Value: body,
	})
}

func (p *Producer) Close() error {
	return p.w.Close()
}
