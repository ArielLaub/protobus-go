# protobus-go documentation

protobus-go runs request/response calls, server streams and published events
over RabbitMQ, with Protocol Buffers on the wire. It is wire-compatible with
the TypeScript and Python ports. New here? Start with
[Getting Started](getting-started.md).

## Guides

| Page | What it covers |
|---|---|
| [Getting Started](getting-started.md) | from an empty directory to a service, a client, an event and a unit test |
| [Services](services.md) | implementing and registering services; concurrency, retries, early ack, priority, instances, interceptors, graceful shutdown |
| [Clients](clients.md) | calling services; call options, timeouts, message ids, fire-and-forget, the dynamic client |
| [Events](events.md) | publishing and subscribing; topics and wildcards, listeners, event retries and the event DLQ |
| [Streaming](streaming.md) | `returns (stream T)` methods; `ServerStream`, ranging over `iter.Seq2`, cancellation, idle timeouts, backpressure |

## Reference

| Page | What it covers |
|---|---|
| [Configuration](configuration.md) | every `Config` field, its environment variable and default; dial options; reconnection |
| [Errors](errors.md) | `HandledError` and `RemoteError`, retries and the DLQ, error codes, sentinel errors, ambiguous publishes |
| [Code generation](codegen.md) | the `protobus` CLI, `protoc-gen-go-protobus`, the generated API, custom types, `protoload` |
| [Testing](testing.md) | the `protobustest` in-memory broker, integration and cross-language suites |

## Operations

| Page | What it covers |
|---|---|
| [Security](security.md) | what `actor` does and does not prove, exposing internal errors, broker credentials |
| [Migration](migration.md) | upgrading from protobus-go v1 to v2 |
| [Compatibility](compatibility.md) | interoperating with TypeScript and Python: type mapping, topology, deliberate differences |

## Elsewhere

- [README](../README.md): the overview and quick start.
- [CHANGELOG](../CHANGELOG.md): what changed in each release.
- [examples/](../examples): `calculator` (RPC and events), `tokenstream`
  (streaming and cancellation), `combat` (service instances and events).
- API reference: [pkg.go.dev](https://pkg.go.dev/github.com/ArielLaub/protobus-go/v2),
  or `go doc -all github.com/ArielLaub/protobus-go/v2` locally.
