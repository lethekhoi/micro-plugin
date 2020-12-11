package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pthethanh/micro-plugins/broker/kafka"
	"github.com/pthethanh/micro/broker"
)

func TestBroker(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	k := kafka.New(kafka.Address("localhost:9092"),
		kafka.Encoder(broker.JSONEncoder{}))

	if err := k.Connect(); err != nil {
		t.Fatal(err)
	}
	defer k.Close(context.Background())

	topic := uuid.New().String()
	ch := make(chan broker.Event)
	sub, err := k.Subscribe(topic, func(msg broker.Event) error {
		ch <- msg
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	sub1, err := k.Subscribe(topic, func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("new"))
	if err != nil {
		panic(err)
	}
	defer sub1.Unsubscribe()
	sub2, err := k.Subscribe(topic, func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("new1"))
	if err != nil {
		panic(err)
	}
	defer sub2.Unsubscribe()
	sub3, err := k.Subscribe(topic, func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("new1"))
	if err != nil {
		panic(err)
	}
	defer sub3.Unsubscribe()

	want := Person{
		Name: "Jack ",
		Age:  1,
	}
	m := mustNewMessage(json.Marshal, want, map[string]string{"type": "person"})
	if err := k.Publish(topic, m); err != nil {
		t.Fatal(err)
	}
	cnt := 0
	go func() {
		for i := range ch {
			cnt++
			if i.Topic() != topic {
				t.Fatalf("got topic=%s, want topic=%s", i.Topic(), topic)
			}
			got := Person{}

			if err := json.Unmarshal(i.Message().Body, &got); err != nil {
				t.Fatalf("got body=%v, want body=%v", got, want)
			}
			if typ, ok := i.Message().Header["type"]; !ok || typ != "person" {
				t.Fatalf("got type=%s, want type=%s", typ, "person")
			}
			fmt.Println("got:", got)
		}
	}()
	time.Sleep(time.Second)
	if cnt != 3 {
		t.Fatalf("got len(messages)=%d, want len(messages)=3", cnt)
	}

}
func mustNewMessage(enc func(v interface{}) ([]byte, error), body interface{}, header map[string]string) *broker.Message {
	b, err := enc(body)
	if err != nil {
		panic(fmt.Sprintf("broker: new message, err: %v", err))
	}
	return &broker.Message{
		Header: header,
		Body:   b,
	}
}
