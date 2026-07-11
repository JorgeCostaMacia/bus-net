# JorgeCostaMacia.Bus.RabbitMQ

**RabbitMQ transport for `JorgeCostaMacia.Bus`** — the concrete command/event bus over [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client/): the `Command`/`Event` base records, the `Transport`, the `CommandContext<T>`/`EventContext<T>`, the ergonomic `CommandHandler<T>`/`EventSubscriber<T>` bases, and the producer/consumer wiring. A Wolverine-style topology — one exchange per message type (a command's `direct`, an event's `fanout`), each handler's queue bound straight to it. **Reference this one package** to run the bus on RabbitMQ — it brings the `JorgeCostaMacia.Bus.*` contracts transitively.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.RabbitMQ.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.RabbitMQ.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.RabbitMQ
```

## Usage

One `AddBusContext` call registers the whole bus over the `Bus:Connection` configuration section. The **producer** lambda maps each message this service sends or publishes to its exchange — a command's `direct`, an event's `fanout`; the optional **consumer** lambda registers this service's handlers, each on its own queue (name it after the consuming group, e.g. `orders.handler`, `billing.on.orders.placed.subscriber`) — omit it for a send-only service. Events mirror commands with the `AddEvent<TEvent>` / `AddEventSubscriber<TEvent, TEventSubscriber>` pair: the fanout exchange copies each event to every bound queue, so one queue per subscribing group.

```csharp
services.AddBusContext(configuration,
    producer => producer.AddCommand<PlaceOrder>("orders"),
    consumer => consumer.AddCommandHandler<PlaceOrder, PlaceOrderHandler>("orders.handler", retryIntervals: [TimeSpan.Zero, TimeSpan.Zero]));
```

A message is a record over the `Command` / `Event` base (the serializer's constructor on top, the convenient one below); a handler closes `CommandHandler<T>` / `EventSubscriber<T>` and is scoped — one instance per delivery. Send and publish through the scoped `IBus`.

```csharp
public sealed record PlaceOrder : Command
{
    public string OrderId { get; init; }

    [JsonConstructor]
    public PlaceOrder(Guid aggregateId, Guid aggregateCorrelationId, DateTime aggregateOccurredAt, ImmutableList<string> aggregateConsumers, string orderId)
        : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
        => OrderId = orderId;

    public PlaceOrder(string orderId)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
        => OrderId = orderId;
}

public sealed class PlaceOrderHandler : CommandHandler<PlaceOrder>
{
    public override Task Handle(CommandContext<PlaceOrder> context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;   // context.Message, context.Transport, and the envelope facets (ids, conversation, retry count, host)
}
```

```csharp
public sealed class OrdersService(IBus bus)
{
    public Task Place(string orderId, CancellationToken cancellationToken = default)
        => bus.Send(new PlaceOrder(orderId), cancellationToken);
}
```

### Configuration

The `Bus:Connection` section configures the single RabbitMQ connection both sides share. `HostName`, `UserName` and `Password` are required (startup fails fast without them); the rest default to: `Ssl` `true` (secure by default — TLS with the host name as the certificate match), `Port` `5671` (the plain 5672 is never a fallback — going plain requires `Ssl` `false` AND an explicit port), `VirtualHost` `/`, `ClientProvidedName` the machine name, `AutomaticRecoveryEnabled` `true`.

```json
{
  "Bus": {
    "Connection": {
      "HostName": "rabbit",
      "UserName": "user",
      "Password": "pass",
      "Port": 5671,
      "VirtualHost": "/",
      "ClientProvidedName": "orders-api",
      "AutomaticRecoveryEnabled": true
    }
  }
}
```

### Retries and park queues

`retryIntervals` is the retry ladder — one delay per attempt. A `00:00` entry re-publishes the delivery to its exchange **immediately**, envelope cloned and `RetryCount` incremented; a **positive** delay parks the delivery through the optional `IRetryScheduler` to be re-published at its time — register [JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz/) to enable it; with no scheduler registered it parks as terminal, since it cannot be delayed. `retryExcludeExceptionTypes` skips the ladder for the listed exception types (inheritance-aware). The default is an empty ladder: the first failure parks.

Each queue gets two durable park queues, unbound — reached by name via the default exchange. Parking is loss-proof: each park redeclares its queue first (one deleted at runtime is recreated on the spot) and publishes `mandatory`, so an unroutable park throws — nacking the original for redelivery — instead of vanishing silently. A terminal handling failure parks to **`{queue}.error`** as a typed error record (the original message fully typed, the whole failure chain and the transport details, with the original envelope cloned in the parked headers); a malformed delivery — undeserializable body, unreadable envelope — and a broken error lane park the raw delivery to **`{queue}.fault`**.

### Topology at startup

The producer declares its own topology: a hosted worker creates every mapped exchange (a command's `direct`, an event's `fanout`) before the app starts serving, so a send-only service does not depend on a consumer having created it. Each consumer declares its exchange, its durable queue bound to it, and its `.error` / `.fault` park queues. Every declare is idempotent — whoever arrives first creates it, the other's declare is a no-op.

## Wire format

Message bodies are JSON serialized with .NET's Web defaults — **camelCase** property names, case-insensitive reads. The envelope travels in **`jcm-`-prefixed headers** (`jcm-message-id`, `jcm-retry-count`, …), hyphenated like the header conventions of HTTP and AMQP (the broker's own `x-*` headers included); dictionary keys in bodies are user data and travel untouched.

## Logging

Log messages are fixed, low-cardinality grouping keys; every variable detail — exchange, queue, body, the decoded envelope headers, the `BusDescription` outcome expansion — travels as structured properties through Serilog's `LogContext`. Wire `.Enrich.FromLogContext()` into the host's Serilog pipeline (the usual default) so those properties reach the sinks.

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Brings [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/) (the transport-agnostic contracts) transitively, plus [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client/).

## About

`JorgeCostaMacia.Bus.RabbitMQ` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
