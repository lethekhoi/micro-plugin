//+build integration_test

package nats_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pthethanh/micro-plugins/broker/nats"
	"github.com/pthethanh/micro/broker"
	"github.com/pthethanh/micro/log"

	natsgo "github.com/nats-io/nats.go"
)

func TestBroker(t *testing.T) {
	b := nats.New(nats.Address("nats://localhost:4222"),
		nats.Encoder(broker.JSONEncoder{}),
		nats.Logger(log.Root().Fields("service", "nats")),
		nats.Options(natsgo.Timeout(2*time.Second)))
	if err := b.Connect(); err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	type Person struct {
		Name string
		Age  int
	}
	ch := make(chan broker.Event, 1)
	// 2 subscribers on the same queue should get 1 message.
	sub, err := b.Subscribe("test", func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("q0"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	sub1, err := b.Subscribe("test", func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("q0"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Unsubscribe()
	// another subscriber on another queue
	sub2, err := b.Subscribe("test", func(msg broker.Event) error {
		ch <- msg
		return nil
	}, broker.Queue("q1"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Unsubscribe()
	// another subscriber without queue
	sub3, err := b.Subscribe("test", func(msg broker.Event) error {
		ch <- msg
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub3.Unsubscribe()
	want := Person{
		Name: "jack",
		Age:  22,
	}
	m := mustNewMessage(json.Marshal, want, map[string]string{"type": "person"})
	if err := b.Publish("test", m); err != nil {
		t.Fatal(err)
	}
	// expect to got 3 messages as we have 2 subscribers on 2 different queues, 1 without queue.
	cnt := 0
	for i := 0; i < 3; i++ {
		e := <-ch
		cnt++
		if e.Topic() != "test" {
			t.Fatalf("got topic=%s, want topic=test", e.Topic())
		}
		got := Person{}
		if err := json.Unmarshal(e.Message().Body, &got); err != nil {
			t.Fatalf("got body=%v, want body=%v", got, want)
		}
		if typ, ok := e.Message().Header["type"]; !ok || typ != "person" {
			t.Fatalf("got type=%s, want type=%s", typ, "person")
		}
	}
	if cnt != 3 {
		t.Fatalf("got len(messages)=%d, want len(messages)=3", cnt)
	}
}

func TestBrokerHealthCheck(t *testing.T) {
	b := nats.New(nats.Address("nats://localhost:4222"),
		nats.Encoder(broker.JSONEncoder{}),
		nats.Logger(log.Root().Fields("service", "nats")),
		nats.Options(natsgo.Timeout(2*time.Second)))
	if err := b.Connect(); err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if err := b.HealthCheck()(context.Background()); err != nil {
		t.Fatalf("got health check failed, want health check success")
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
