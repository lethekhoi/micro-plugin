package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pthethanh/micro/broker"
)

type (
	event struct {
		op      *broker.SubscribeOptions
		t       string
		m       *broker.Message
		mes     *sarama.ConsumerMessage
		session sarama.ConsumerGroupSession
	}
	subscriber struct {
		queue string
		t     string
		s     sarama.Consumer
	}
	subscriberGroup struct {
		queue string
		t     string
		g     sarama.ConsumerGroup
	}
)

func (e *event) Topic() string {
	return e.t
}

func (e *event) Message() *broker.Message {
	return e.m
}

func (e *event) Ack() error {
	if e.op.AutoAck == true {
		e.session.MarkMessage(e.mes, "")
	}
	return nil
}

func (s *subscriber) Topic() string {
	return s.t
}

func (s *subscriber) Unsubscribe() error {
	if err := s.s.Close(); err != nil {
		return err
	}
	return nil
}
func (s *subscriberGroup) Topic() string {
	return s.t
}

func (s *subscriberGroup) Unsubscribe() error {

	if err := s.g.Close(); err != nil {
		return err
	}
	return nil
}
