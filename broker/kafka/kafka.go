package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pthethanh/micro/broker"
	"github.com/pthethanh/micro/log"
)

type (
	// Kafka is an implementation of broker.Broker using Kafka.
	Kafka struct {
		sub           chan *broker.Message
		addrs         string
		producer      sarama.SyncProducer
		consumer      sarama.Consumer
		log           log.Logger
		encoder       broker.Encoder
		config        *sarama.Config
		client        sarama.Client
		consumerGroup sarama.ConsumerGroup
	}
	// Option is an optional configuration.
	Option func(*Kafka)
)

// New return a new Kafka message broker.
// If address is not set, default address "localhost:9092" will be used.
func New(opts ...Option) *Kafka {
	k := &Kafka{}
	for _, opt := range opts {
		opt(k)
	}
	if k.addrs == "" {
		k.addrs = "localhost:9092"
	}
	if k.encoder == nil {
		k.encoder = broker.JSONEncoder{}
	}
	if k.log == nil {
		k.log = log.Root()
	}
	return k
}

// Connect connect to target server.
func (k *Kafka) Connect() error {
	var err error

	k.config = k.getConfig()

	brokers := strings.Split(k.addrs, ",")
	if k.client, err = sarama.NewClient(brokers, k.config); err != nil {
		return err
	}
	// Create new producer
	if k.producer, err = sarama.NewSyncProducerFromClient(k.client); err != nil {
		return err
	}

	return nil

}

// Publish implements broker.Broker interface.
func (k *Kafka) Publish(topic string, m *broker.Message, opts ...broker.PublishOption) error {
	if k.encoder == nil {
		return ErrMissingEncoder
	}

	b, err := k.encoder.Encode(m)

	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(b),
	}
	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Infof("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}

// Subscribe implements broker.Broker interface.
func (k *Kafka) Subscribe(topic string, h broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {

	op := &broker.SubscribeOptions{
		AutoAck: true,
	}
	op.Apply(opts...)

	var err error
	//handler function
	handler := func(msg *sarama.ConsumerMessage) {
		m := broker.Message{}
		if err := k.encoder.Decode(msg.Value, &m); err != nil {
			log.Errorf("kafka: subscribe: decode failed, err: %v", err)
			return
		}
		h(&event{
			op: op,
			t:  topic,
			m:  &m,
		})
	}
	//Consumer with no groupID
	if op.Queue == "" {
		log.Info("consumer with no groupID")
		// Create new consumer
		k.consumer, err = sarama.NewConsumer(strings.Split(k.addrs, ","), k.config)
		if err != nil {
			return nil, err
		}
		partitionList, err := k.consumer.Partitions(topic)
		if err != nil {
			return nil, err
		}
		for partition := range partitionList {
			consumer, err := k.consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			go func() {
				for msg := range consumer.Messages() {
					handler(msg)
				}
			}()
		}
		return &subscriber{
			queue: op.Queue,
			t:     topic,
			s:     k.consumer,
		}, nil

	} //end no group
	{
		// Create new consumer group
		if k.consumerGroup, err = sarama.NewConsumerGroup([]string{k.addrs}, op.Queue, k.config); err != nil {
			return nil, err
		}
		ctx := context.Background()
		consumer := Consumer{
			encoder: k.encoder,
			topic:   topic,
			h:       h,
			ready:   make(chan bool),
		}
		go func() {
			for {

				if err := k.consumerGroup.Consume(ctx, []string{topic}, &consumer); err != nil {
					log.Panicf("Error from consumer: %v", err)
				}
				<-ctx.Done()

				if ctx.Err() != nil {
					return
				}
			}
		}()
		<-consumer.ready // Await till the consumer has been set up
		log.Info("Sarama consumer up and running!...")

		return &subscriberGroup{
			queue: op.Queue,
			t:     topic,
			g:     k.consumerGroup,
		}, nil
	}

}

//Close the SyncProducer and Client
func (k *Kafka) Close(ctx context.Context) error {
	var err error
	if err = k.producer.Close(); err != nil {
		return err
	}
	if err = k.client.Close(); err != nil {
		return err
	}

	return err
}

//Get a new Config
func (k *Kafka) getConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.MaxVersion
	return config

}
