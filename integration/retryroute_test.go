package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/internal/brokertest"
	"github.com/ArielLaub/protobus-go/v2/internal/gentest"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// A failed request is retried on the route the broker delivered it on.
// Publisher-supplied routing metadata (x-original-routing-key, and the CC
// and BCC keys RabbitMQ also applies when it dead-letters a message back from
// the retry queue) must not move the retry anywhere else.
func TestRetryStaysOnTheDeliveredRoute(t *testing.T) {
	for name, tc := range map[string]struct {
		headers amqp.Table
		victim  int // copies the victim queue may hold: the publisher's own CC reaches it once
	}{
		"forged original routing key": {amqp.Table{"x-original-routing-key": "REQUEST.Victim.wipe"}, 0},
		"forged CC":                   {amqp.Table{"CC": []any{"REQUEST.Victim.wipe"}}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			vh := brokertest.Require(t).NewVHost(t)
			bus := dial(t, vh.URL())
			impl := newCalc()
			serve(t, bus, impl, protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 3, Delay: 50 * time.Millisecond}))

			conn, err := amqp.Dial(vh.URL())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ch, err := conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ch.QueueDeclare("Victim", true, false, false, false, nil); err != nil {
				t.Fatal(err)
			}
			if err := ch.QueueBind("Victim", "REQUEST.Victim.*", "proto.bus", false, nil); err != nil {
				t.Fatal(err)
			}
			data, _ := proto.Marshal(&gentest.FailRequest{Mode: "transient", Message: "flaky", SucceedAfter: 1})
			err = ch.PublishWithContext(context.Background(), "proto.bus", "REQUEST.GenTest.Calc.fail", false, false, amqp.Publishing{
				MessageId: "m1", CorrelationId: "c1", Headers: tc.headers,
				Body: wire.AppendRequest(nil, wire.Request{Method: "GenTest.Calc.fail", Data: data}),
			})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				if v, ok := impl.attempts.Load("m1"); ok && v.(interface{ Load() int32 }).Load() == 2 {
					break // the retry came back to GenTest.Calc and succeeded
				}
				if time.Now().After(deadline) {
					t.Fatal("the retry never came back to the service")
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(300 * time.Millisecond)
			if n, _, err := vh.Depth("Victim"); err != nil || n != tc.victim {
				t.Fatalf("the retry leaked to another service's queue: %d copies (want %d), %v", n, tc.victim, err)
			}
		})
	}
}
