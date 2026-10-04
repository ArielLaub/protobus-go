# Events

Events are fan-out messages: a publisher announces something happened, and
every interested party gets its own copy. They travel on the topic exchange
`proto.bus.events` (`EVENTS_EXCHANGE_NAME`). Every queue bound to a matching
topic receives a copy; processes sharing one queue compete for it, so each
event is handled once per queue.

The snippets use the calculator example's generated package
(`examples/calculator/gen/calculator`), whose `Calculated` message is the
event, and import the library as `protobus`.

## Publishing

```go
func announce(ctx context.Context, bus *protobus.Bus, op string) error {
	ev := &calculator.Calculated{Operation: op, At: pbtypes.NewTimestamp(time.Now())}
	return bus.PublishEvent(ctx, ev)
}
```

`PublishEvent` returns once the broker has confirmed the event. An event that
no queue is bound to is not an error: events are not published `mandatory`.

- The event's **type** is the message's fully-qualified protobuf name
  (`Calculator.Calculated`). Subscribers decode it with that type.
- Its **topic**, the routing key, defaults to `EVENT.<type>`;
  `protobus.EventTopic(msg)` returns it.
- Events are persistent and carry a fresh `correlationId`, and a `messageId`
  that is a fresh UUID unless set.

| Option | Effect |
|---|---|
| `WithTopic(topic)` | publish under `topic` instead of `EVENT.<type>` |
| `WithMessageID(id)` | the event's identity, as subscribers see it in `EventInfo.MessageID`: non-blank, at most 255 bytes, carried unchanged across retries and to the DLQ |

```go
func announceOrder(ctx context.Context, bus *protobus.Bus, region, orderID string) error {
	ev := &calculator.Calculated{Operation: "order", At: pbtypes.NewTimestamp(time.Now())}
	return bus.PublishEvent(ctx, ev,
		protobus.WithTopic("ORDERS."+region+".CREATED"),
		protobus.WithMessageID("order-created-"+orderID))
}
```

A failed publish returns the same errors as a call's publish: a
`*PublishError` (check `Ambiguous()` before republishing, and reuse the
message id when you do), `ErrNotReady` or `ErrClosed`; an invalid message id
fails with `ErrInvalidMessageID` before anything is sent. See
[Clients](clients.md#ambiguous-outcomes-and-idempotency).

## Listeners

An `EventListener` consumes one queue and runs the handlers whose topic
patterns match each delivery. There are two ways to get one.

**A service's own listener**, `svc.Events()`, on the durable queue
`<service name>.Events`. It starts and stops with the service, and takes its
options from `Register` (see [Services](services.md#service-events)).

**A standalone listener**, `bus.NewEventListener(queue, opts...)`:

- a named queue is durable and shared: every process listening under that
  name competes for its events;
- an empty name gives this process a private, server-named queue (exclusive,
  auto-delete) that disappears with its connection. It cannot use retries.

```go
func listen(ctx context.Context, bus *protobus.Bus) (*protobus.EventListener, error) {
	l, err := bus.NewEventListener("Audit.Events", protobus.WithEventConcurrency(4))
	if err != nil {
		return nil, err
	}
	err = protobus.Subscribe(ctx, l, func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
		slog.InfoContext(ctx, "calculated", "operation", ev.Operation, "messageId", info.MessageID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, l.Start(ctx)
}
```

| Method | What it does |
|---|---|
| `Start(ctx)` | Declares the queue and its bindings and starts consuming. Idempotent; a failed start can be retried. |
| `StopConsuming(ctx)` | Stops taking events; events in hand still settle. `Bus.Shutdown` calls it for every started listener. |
| `Close()` | Stops at once. Running handlers are cancelled and their events redelivered. Idempotent. |
| `Queue()` | The queue name; for a private queue, the broker-assigned name once started. |

Subscribe before or after `Start`. A subscription made after `Start` adds its
binding to the live queue at once.

## Subscribing

`protobus.Subscribe[T]` runs a handler for every event of type `T` delivered
under the subscription's topic, by default `EVENT.<T's full name>`. `T` is a
type parameter (Go generics); it is inferred from the handler's second
parameter, so `Subscribe(ctx, l, func(ctx, ev *calculator.Calculated, info) error {...})`
subscribes to `Calculated` events without naming the type twice.

| Function | Handler receives | Default binding |
|---|---|---|
| `Subscribe[T](ctx, l, handler, opts...)` | a `T` | `EVENT.<T>` |
| `SubscribeType(ctx, l, messageType, handler, opts...)` | a `proto.Message` of `messageType`, such as a `dynamicpb` type from `protoload` | `EVENT.<type>` |
| `l.SubscribeAll(ctx, handler)` | a `proto.Message` of whatever type the event names, from the bus's type registry | `#` (every event) |

```go
func subscribeEverything(ctx context.Context, l *protobus.EventListener) error {
	return l.SubscribeAll(ctx, func(ctx context.Context, msg proto.Message, info protobus.EventInfo) error {
		slog.InfoContext(ctx, "event", "type", info.Type, "routingKey", info.RoutingKey)
		return nil
	})
}
```

A typed handler skips an event of another type that matches its topic. An
event that reaches the queue but runs no handler at all is acknowledged and
logged ("no handler for event", with its type and routing key only). A
`SubscribeAll` handler that receives a type the registry does not know treats
the event as undecodable (see [Failures](#failures)).

There is no unsubscribe. A binding added to a durable queue stays on it at the
broker, so a subscription removed from the code still brings its events to the
queue, where they are acknowledged as having no handler, until an operator
removes the binding.

## Topics and wildcards

`WithTopic(pattern)` subscribes to a topic pattern instead of the default.
Topics are words separated by dots; patterns follow AMQP topic rules: `*`
matches exactly one word, `#` zero or more.

```go
func subscribeOrders(ctx context.Context, l *protobus.EventListener) error {
	created := func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
		slog.InfoContext(ctx, "order created", "topic", info.RoutingKey)
		return nil
	}
	if err := protobus.Subscribe(ctx, l, created, protobus.WithTopic("ORDERS.*.CREATED")); err != nil {
		return err
	}
	everything := func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
		return nil
	}
	return protobus.Subscribe(ctx, l, everything, protobus.WithTopic("ORDERS.#"))
}
```

An event published under `ORDERS.US.CREATED` runs both handlers, in that
order; `ORDERS.US` runs only the second. Each distinct pattern becomes one
binding of the listener's queue.

Handlers are matched against the routing key the broker delivered the event
on, not against the topic in the event's body, which the publisher controls.
A publisher allowed to publish only under `PUBLIC.#` therefore cannot reach a
handler bound to `ADMIN.#` by writing `ADMIN.reset` into the body.

## Order and EventInfo

For each delivery, the matching handlers run one after another, on the
delivery's goroutine: `SubscribeAll` handlers first, then typed ones in the
order they were subscribed. **The first handler to fail stops the rest.**

Every handler receives an `EventInfo`:

| Field | Meaning |
|---|---|
| `Type` | the event's fully-qualified message name |
| `Topic` | the topic the publisher wrote in the body |
| `RoutingKey` | the key the broker delivered on, which handlers are matched against |
| `MessageID` | the event's identity, the same on every retry hop: deduplicate on it |
| `CorrelationID` | the publish's correlation id |
| `Redelivered` | the broker has delivered this message before |
| `Attempt` | retry hops so far (with `WithEventRetry`); 0 for the first delivery |
| `Headers` | a copy of the AMQP headers |

## Concurrency

`WithEventConcurrency(n)` sets how many events a listener handles at once,
each on its own goroutine. It is the queue's prefetch and defaults to
`Config.DefaultPrefetch` (`DEFAULT_PREFETCH`, default 1), so by default a
listener handles one event at a time. It is accepted by `NewEventListener` and,
for a service's listener, by `Register`. Each handler attempt is bounded by
`Config.ProcessingTimeout`; an overrun counts as a failure.

## Failures

What happens to an event depends on what its handlers did and on whether the
listener has retries:

| Outcome | Without retries (default) | With `WithEventRetry` |
|---|---|---|
| Every matching handler returned nil | acknowledged | acknowledged |
| A handler returned an error, panicked or overran the timeout | rejected: dropped | retried; after `MaxRetries`, to `<queue>.DLQ` |
| A handler returned a `HandledError` | rejected: dropped | to `<queue>.DLQ` at once, not retried |
| The event did not decode (envelope, payload, or a type `SubscribeAll` cannot resolve) | rejected: dropped | to `<queue>.DLQ` at once, not retried |

Retries are off by default so that one poisonous event cannot stall a
subscriber. A dropped event is logged; nothing else keeps it.

## Event retries

```go
func retryingListener(ctx context.Context, bus *protobus.Bus) (*protobus.EventListener, error) {
	return bus.NewEventListener("Billing.Events",
		protobus.WithEventRetry(protobus.EventRetryPolicy{MaxRetries: 3, Delay: 10 * time.Second}))
}
```

`EventRetryPolicy` has `MaxRetries` (retries after the first attempt; 0 turns
retries off) and `Delay` (how long a failed event waits; required with
retries). It needs a named queue: `NewEventListener("")` with retries fails.
Pass it to `Register` for a service's own listener.

### Topology

For a listener on queue `<queue>`, `Start` declares:

| Name | Kind | Arguments |
|---|---|---|
| `<queue>.Redelivery` | durable topic exchange, bound to `<queue>` with `#` | |
| `<queue>.Retry.Exchange` | durable topic exchange | |
| `<queue>.Retry` | durable queue, bound to the retry exchange with `#` | `x-message-ttl` = `Delay`, `x-dead-letter-exchange` = `<queue>.Redelivery` |
| `<queue>.DLQ` | durable queue | |

A failed event is published to `<queue>.Retry.Exchange` under its original
routing key and waits on `<queue>.Retry`. When its TTL expires, RabbitMQ
dead-letters it to `<queue>.Redelivery`, which is bound only to this
listener's queue. It does not go back to `proto.bus.events`: that would hand a
second copy to every other subscriber of the topic, including those that
handled it. Keeping the routing key means the same handlers match it again.

The copies carry the headers of the service retry ladder (`x-retry-count`,
`x-original-routing-key`, `x-first-failure-time`, `x-last-error`, and on the
DLQ `x-original-queue` and `x-dlq-time`); see
[Services](services.md#headers). As there, changing `Delay` on an existing
retry queue fails `Start` with `ErrRetryQueueMismatch`.

### A retry re-runs every matching handler

The retry is per listener queue, not per handler. When one of several handlers
on a listener fails, the redelivered event runs **every** matching handler on
that listener again, including the ones that succeeded the first time (and,
since the first failure stops the rest, the ones that never ran). Other
listeners and services that handled the event are not affected; they have
their own queues.

So on a listener with retries:

- make every handler idempotent, deduplicating on `EventInfo.MessageID`,
  which stays the same across every hop; or
- give handlers that must retry independently a listener (and queue) each.

```go
type ledger struct {
	mu   sync.Mutex
	seen map[string]bool // in production, a database table with a unique key
}

func (lg *ledger) onCalculated(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if lg.seen[info.MessageID] {
		return nil // already applied on an earlier attempt
	}
	// ... apply the event ...
	lg.seen[info.MessageID] = true
	return nil
}

func subscribeLedger(ctx context.Context, l *protobus.EventListener, lg *ledger) error {
	return protobus.Subscribe(ctx, l, lg.onCalculated)
}
```

Passing a method value (`lg.onCalculated`) as the handler binds it to `lg`.
