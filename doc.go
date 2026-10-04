// Package protobus runs microservices over RabbitMQ with Protocol Buffers on
// the wire: request/response calls, server streams and published events.
//
// It is the Go port of protobus (TypeScript) and protobus-py, and is
// wire-compatible with both: services and clients in all three languages
// share one broker, one schema and even one queue.
//
// # Services and clients
//
// A service is defined in a .proto file. The protobus CLI (or
// protoc-gen-go-protobus) generates the messages plus gRPC-style bindings:
// an XServer interface to implement, RegisterXServer to put it on a Bus, and
// an XClient to call it.
//
//	bus, err := protobus.Dial(ctx, "amqp://guest:guest@localhost:5672/")
//	if err != nil { ... }
//	defer bus.Close()
//
//	svc, err := calculator.RegisterServiceServer(bus, &server{}, protobus.WithMaxConcurrent(8))
//	if err != nil { ... }
//	err = protobus.Run(ctx, bus, svc) // serve until ctx ends or SIGTERM, then drain
//
// and, in any process on the same broker:
//
//	client := calculator.NewServiceClient(bus)
//	sum, err := client.Add(ctx, &calculator.AddRequest{A: 20, B: 22})
//
// Each service owns one durable queue that every replica consumes from, so
// the broker balances load and fails over. Within a process, every
// unacknowledged delivery is handled on its own goroutine, up to the
// service's prefetch (WithMaxConcurrent): handlers run truly in parallel.
//
// A handler's error decides what happens to the request. A *HandledError is
// an answer: it reaches the caller as a *RemoteError carrying its code and
// is never retried. Any other error, a panic or a processing timeout is a
// failure: the request is retried through the service's retry queue
// (RetryPolicy) and dead-lettered once its retries are spent.
//
// # Streams
//
// A method declared `returns (stream T)` is a server stream. The handler
// sends chunks through a ServerStream; the client ranges over an
// iter.Seq2, and breaking out of the loop or ending its context cancels the
// stream on the server.
//
// # Events
//
// Bus.PublishEvent publishes a message on the events exchange under the
// topic EVENT.<message full name>. An EventListener (or a Service's own
// Events listener) subscribes with Subscribe, by message type, topic pattern
// or both, and retries and dead-letters failing events like requests.
//
// # Reliability
//
// Every publish waits for the broker's confirm and, where an unroutable
// message would be lost, is mandatory: a nil error means the broker has
// it. Errors that leave the outcome unknown are *PublishError values whose
// Ambiguous method reports true; give such calls a stable WithMessageID to
// make them safe to repeat. A lost connection is re-established
// automatically (see Config.Reconnect and WithConnectionObserver): every
// service and listener resumes on the new one, and calls made meanwhile wait
// for it, up to Config.ConnectionReadyTimeout. A call already awaiting its
// reply when the connection drops fails with ErrDisconnected, since its
// request may or may not have been processed.
//
// # Testing
//
// The protobustest package provides an in-memory broker, so services and
// clients can be unit-tested without RabbitMQ.
package protobus
