// Package brokertest gives tests an isolated RabbitMQ: a fresh virtual host
// per test on the broker named by PROTOBUS_TEST_AMQP_URL, created and
// deleted through the management API at PROTOBUS_TEST_MGMT_URL.
//
// There is deliberately no default broker URL. localhost:5672 is routinely a
// port-forward to a shared cluster, and a test suite that declares, purges
// and deletes queues must never reach one by accident. Tests that need a
// broker skip when the variables are unset.
//
//	docker compose up -d
//	PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ \
//	PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673 go test ./...
package brokertest

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// httpClient keeps no idle connections, so leak checks in tests see none.
var httpClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}

// Broker is a RabbitMQ reachable for tests.
type Broker struct {
	amqp *url.URL
	mgmt *url.URL
}

// Require returns the test broker or skips the test.
func Require(t testing.TB) *Broker {
	t.Helper()
	b, err := FromEnv()
	if err != nil {
		t.Skip(err)
	}
	return b
}

// FromEnv reads the broker from the environment.
func FromEnv() (*Broker, error) {
	rawAMQP, rawMgmt := os.Getenv("PROTOBUS_TEST_AMQP_URL"), os.Getenv("PROTOBUS_TEST_MGMT_URL")
	if rawAMQP == "" || rawMgmt == "" {
		return nil, fmt.Errorf("PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set; skipping broker tests")
	}
	a, err := url.Parse(rawAMQP)
	if err != nil {
		return nil, err
	}
	m, err := url.Parse(rawMgmt)
	if err != nil {
		return nil, err
	}
	b := &Broker{amqp: a, mgmt: m}
	if _, err := b.api(http.MethodGet, "/api/overview", nil); err != nil {
		return nil, fmt.Errorf("management API unreachable: %w", err)
	}
	return b, nil
}

func (b *Broker) api(method, path string, body any) ([]byte, error) {
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(buf)
	}
	u := *b.mgmt
	u.User = nil
	req, err := http.NewRequest(method, u.String()+path, r)
	if err != nil {
		return nil, err
	}
	if b.mgmt.User != nil {
		pw, _ := b.mgmt.User.Password()
		req.SetBasicAuth(b.mgmt.User.Username(), pw)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, out)
	}
	return out, nil
}

// VHost is a virtual host created for one test.
type VHost struct {
	b    *Broker
	Name string
}

// NewVHost creates a fresh virtual host, deleted when the test ends.
func (b *Broker) NewVHost(t testing.TB) *VHost {
	t.Helper()
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	name := "protobus-go-test-" + hex.EncodeToString(rnd[:])
	if _, err := b.api(http.MethodPut, "/api/vhosts/"+url.PathEscape(name), map[string]any{}); err != nil {
		t.Fatal(err)
	}
	user := b.amqp.User.Username()
	perms := map[string]string{"configure": ".*", "write": ".*", "read": ".*"}
	if _, err := b.api(http.MethodPut, "/api/permissions/"+url.PathEscape(name)+"/"+url.PathEscape(user), perms); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = b.api(http.MethodDelete, "/api/vhosts/"+url.PathEscape(name), nil) })
	return &VHost{b: b, Name: name}
}

// URL is the AMQP URL of the virtual host.
func (v *VHost) URL() string {
	u := *v.b.amqp
	u.Path = "/" + v.Name
	u.RawPath = "/" + url.PathEscape(v.Name)
	return u.String()
}

// Connection describes a client connection, as the management API reports it.
type Connection struct {
	Name       string         `json:"name"`
	Timeout    int            `json:"timeout"` // negotiated heartbeat, seconds
	Properties map[string]any `json:"client_properties"`
}

// Connections lists the connections to the virtual host.
func (v *VHost) Connections() ([]Connection, error) {
	raw, err := v.b.api(http.MethodGet, "/api/vhosts/"+url.PathEscape(v.Name)+"/connections", nil)
	if err != nil {
		return nil, err
	}
	var out []Connection
	return out, json.Unmarshal(raw, &out)
}

// WaitConnections waits until at least n connections are open.
func (v *VHost) WaitConnections(t testing.TB, n int) []Connection {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		cs, err := v.Connections()
		if err == nil && len(cs) >= n {
			return cs
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited for %d connections, have %d (%v)", n, len(cs), err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// KillConnections force-closes every connection to the virtual host, as a
// broker failure would.
func (v *VHost) KillConnections(t testing.TB) int {
	t.Helper()
	cs, err := v.Connections()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cs {
		if _, err := v.b.api(http.MethodDelete, "/api/connections/"+url.PathEscape(c.Name), nil); err != nil {
			t.Fatal(err)
		}
	}
	return len(cs)
}

// Queue describes a queue.
type Queue struct {
	Name       string         `json:"name"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Exclusive  bool           `json:"exclusive"`
	Arguments  map[string]any `json:"arguments"`
	Messages   int            `json:"messages"`
	Consumers  int            `json:"consumers"`
}

// Queue fetches a queue, reporting whether it exists.
func (v *VHost) Queue(name string) (Queue, bool) {
	raw, err := v.b.api(http.MethodGet, "/api/queues/"+url.PathEscape(v.Name)+"/"+url.PathEscape(name), nil)
	if err != nil {
		return Queue{}, false
	}
	var q Queue
	return q, json.Unmarshal(raw, &q) == nil
}

// Message is a message read back from a queue.
type Message struct {
	RoutingKey string `json:"routing_key"`
	Payload    string `json:"payload"`
	Encoding   string `json:"payload_encoding"`
	Properties struct {
		MessageID     string         `json:"message_id"`
		CorrelationID string         `json:"correlation_id"`
		ReplyTo       string         `json:"reply_to"`
		ContentType   string         `json:"content_type"`
		Priority      int            `json:"priority"`
		DeliveryMode  int            `json:"delivery_mode"`
		Headers       map[string]any `json:"headers"`
	} `json:"properties"`
}

// Peek reads up to n messages from a queue without consuming them.
func (v *VHost) Peek(t testing.TB, queue string, n int) []Message {
	t.Helper()
	body := map[string]any{"count": n, "ackmode": "ack_requeue_true", "encoding": "base64"}
	raw, err := v.b.api(http.MethodPost, "/api/queues/"+url.PathEscape(v.Name)+"/"+url.PathEscape(queue)+"/get", body)
	if err != nil {
		t.Fatal(err)
	}
	var out []Message
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// Depth reports a queue's ready messages and consumers, read from the broker
// itself with a passive declare: the management API's statistics refresh only
// every few seconds.
func (v *VHost) Depth(queue string) (messages, consumers int, err error) {
	conn, err := amqp.Dial(v.URL())
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return 0, 0, err
	}
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		return 0, 0, err
	}
	return q.Messages, q.Consumers, nil
}

// WaitQueueDepth waits until the queue holds n ready messages.
func (v *VHost) WaitQueueDepth(t testing.TB, queue string, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		m, _, err := v.Depth(queue)
		if err == nil && m == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s: waited for %d messages, have %d (%v)", queue, n, m, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WaitConsumers waits until the queue has n consumers.
func (v *VHost) WaitConsumers(t testing.TB, queue string, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, c, err := v.Depth(queue)
		if err == nil && c == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %s: waited for %d consumers, have %d (%v)", queue, n, c, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
