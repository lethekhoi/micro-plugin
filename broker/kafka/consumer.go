package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pthethanh/micro/broker"
	"github.com/pthethanh/micro/log"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	op      *broker.SubscribeOptions
	encoder broker.Encoder
	topic   string
	h       broker.Handler
	ready   chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkOffset(consumer.topic, message.Partition, message.Offset, "")
		log.Infof("Message claimed: value = %s, timestamp = %v, topic = %s, , offser = %d", string(message.Value), message.Timestamp, message.Topic, message.Offset)
		m := broker.Message{}

		if err := consumer.encoder.Decode(message.Value, &m); err != nil {
			log.Errorf("kafka: subscribe: decode failed, err: %v", err)
			return err
		}
		consumer.h(&event{
			op:      consumer.op,
			t:       consumer.topic,
			m:       &m,
			session: session,
		})
	}
	return nil
}
