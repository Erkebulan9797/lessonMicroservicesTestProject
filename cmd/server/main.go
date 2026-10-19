package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"debit-notifier/internal/account"
	"debit-notifier/internal/api"
	"debit-notifier/internal/events"
	"debit-notifier/internal/notify"
)

const topic = "debit-events"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpAddr := env("HTTP_ADDR", ":8080")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("redis: ", err)
	}

	if err := events.EnsureTopics(brokers[0], topic, topic+".DLT"); err != nil {
		log.Fatal("kafka topics: ", err)
	}

	accounts := account.New(rdb)
	notifier := notify.New(accounts, notify.Email{}, notify.SMS{})

	producer := events.NewProducer(brokers, topic)
	defer producer.Close()

	consumer := events.NewConsumer(brokers, topic, "notifier", rdb, notifier)
	defer consumer.Close()
	go consumer.Run(ctx)

	srv := &http.Server{Addr: httpAddr, Handler: api.New(accounts, producer)}
	go func() {
		log.Println("слушаю", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("останавливаюсь")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
