package events

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"debit-notifier/internal/model"
)

const (
	retries    = 3
	retryPause = 2 * time.Second
	dedupTTL   = 24 * time.Hour
)

type Notifier interface {
	NotifyDebit(ctx context.Context, e model.DebitEvent) error
}

type Consumer struct {
	r        *kafka.Reader
	dlt      *kafka.Writer
	rdb      *redis.Client
	notifier Notifier
}

func NewConsumer(brokers []string, topic, group string, rdb *redis.Client, n Notifier) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     group,
			StartOffset: kafka.FirstOffset,
		}),
		dlt: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic + ".DLT",
		},
		rdb:      rdb,
		notifier: n,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("fetch:", err)
			time.Sleep(time.Second)
			continue
		}

		if err := c.handleWithRetry(ctx, msg); err != nil {
			log.Printf("сообщение offset=%d уходит в DLT: %v", msg.Offset, err)
			if derr := c.dlt.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value}); derr != nil {
				// в DLT не записалось - не коммитим, пусть прочитается ещё раз
				log.Println("dlt:", derr)
				continue
			}
		}

		if err := c.r.CommitMessages(ctx, msg); err != nil {
			log.Println("commit:", err)
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message) error {
	var err error
	for i := 0; i < retries; i++ {
		if err = c.handle(ctx, msg); err == nil {
			return nil
		}
		log.Printf("попытка %d/%d: %v", i+1, retries, err)
		select {
		case <-time.After(retryPause):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var e model.DebitEvent
	if err := json.Unmarshal(msg.Value, &e); err != nil {
		return errors.New("битый json: " + err.Error())
	}

	// кафка может доставить дважды, поэтому метим событие в редисе
	key := "notified:" + e.EventID
	first, err := c.rdb.SetNX(ctx, key, "1", dedupTTL).Result()
	if err != nil {
		return err
	}
	if !first {
		log.Printf("событие %s уже было, пропускаю", e.EventID)
		return nil
	}

	if err := c.notifier.NotifyDebit(ctx, e); err != nil {
		// снимаем метку, иначе повтор пропустится
		c.rdb.Del(ctx, key)
		return err
	}
	return nil
}

func (c *Consumer) Close() {
	c.r.Close()
	c.dlt.Close()
}
