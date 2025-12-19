# Protobus Go Documentation

Welcome to the protobus-go documentation. This guide covers everything you need to build robust microservices with RabbitMQ and Protocol Buffers in Go.

## Quick Navigation

### Getting Started
- [Getting Started](getting-started.md) - Your first protobus-go service
- [Architecture](architecture.md) - System design overview
- [Configuration](configuration.md) - Environment and connection settings

### API Reference
- [Context](api/context.md) - Connection and factory management
- [BaseService](api/base-service.md) - Foundation for all services
- [RunnableService](api/runnable-service.md) - Service with lifecycle management
- [ServiceProxy](api/service-proxy.md) - RPC client for calling services
- [ServiceCluster](api/service-cluster.md) - Managing multiple service instances

### Advanced Topics
- [Error Handling](advanced/error-handling.md) - HandledError and retries
- [Custom Types](advanced/custom-types.md) - BigInt, Timestamp, and custom serialization

### CLI Tools
- [CLI Documentation](cli.md) - Code generation and scaffolding

## Core Concepts

### RabbitMQ-Native Architecture

Unlike transport-agnostic frameworks, protobus-go leverages RabbitMQ's native capabilities:

| Feature | Traditional Frameworks | Protobus |
|---------|----------------------|----------|
| Load balancing | App-level round-robin | Broker-level competing consumers |
| Message routing | App-level pattern matching | Native topic exchanges |
| Reliability | Custom retry logic | Native acknowledgments |
| Dead letters | Manual implementation | Native DLX support |

### Message Flow

```
Client                    RabbitMQ                   Service
  │                          │                          │
  │  Call("Add", {a:1,b:2}) │                          │
  │─────────────────────────>│                          │
  │                          │  REQUEST.Service.Add     │
  │                          │─────────────────────────>│
  │                          │                          │ handler()
  │                          │         Response         │
  │                          │<─────────────────────────│
  │       {result: 3}        │                          │
  │<─────────────────────────│                          │
```

### Cross-Language Compatibility

Protobus services are fully interoperable across languages:
- **Go**: protobus-go (this library)
- **TypeScript/Node.js**: [protobus](https://github.com/ArielLaub/protobus)
- **Python**: [protobus-py](https://github.com/ArielLaub/protobus-py)

Services in different languages communicate seamlessly via RabbitMQ.
