package events

import (
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// создаём топики если их ещё нет
func EnsureTopics(broker string, topics ...string) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()

	cfgs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		cfgs = append(cfgs, kafka.TopicConfig{Topic: t, NumPartitions: 3, ReplicationFactor: 1})
	}
	return cc.CreateTopics(cfgs...)
}
