# JorgeCostaMacia.Bus.Kafka

**Kafka transport for `JorgeCostaMacia.Bus`** — the concrete command/event bus over [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/): the `Command`/`Event` base records, the `Transport`, the `CommandContext<T>`/`EventContext<T>`, the ergonomic `CommandHandler<T>`/`EventSubscriber<T>` bases, and the producer/consumer wiring. A topic-per-message-type topology — each message maps to one topic, each handler runs as its own consumer group holding its own offsets; commands are consumed by a single group (point-to-point), events by one group per subscriber (pub/sub, since Kafka delivers every record to every group on the topic). **Reference this one package** to run the bus on Kafka — it brings the `JorgeCostaMacia.Bus.*` contracts transitively.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Kafka.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Kafka.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Kafka
```

## Usage

One `AddBusContext` call registers the whole bus over the `Bus:Producer` / `Bus:Consumer` configuration sections. The **producer** lambda maps each message this service sends or publishes to its topic; the optional **consumer** lambda registers this service's handlers, each on its own consumer group (name it after the consuming group, e.g. `orders.handler`, `billing.on.orders.placed.subscriber`) — omit it for a send-only service, which then needs no `Bus:Consumer` section. Events mirror commands with the `AddEvent<TEvent>` / `AddEventSubscriber<TEvent, TEventSubscriber>` pair: Kafka delivers each record on the topic to every consumer group subscribed to it, so one group per subscribing service.

```csharp
services.AddBusContext(configuration,
    producer => producer.AddCommand<PlaceOrder>("orders"),
    consumer => consumer.AddCommandHandler<PlaceOrder, PlaceOrderHandler>("orders.handler", retryIntervals: [TimeSpan.Zero, TimeSpan.Zero]));
```

A message is a record over the `Command` / `Event` base (the serializer's constructor on top, the convenient one below); a handler closes `CommandHandler<T>` / `EventSubscriber<T>` and is scoped — one instance per delivery. Delivery is **at-least-once and unordered**, so handlers must be idempotent — deduplicate by the message id or reconcile by its timestamp. Send and publish through the scoped `IBus`.

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

The two sides bind independently: the **`Bus:Producer`** section configures the shared producer (required always), the **`Bus:Consumer`** section configures the consumers (required only when a consumer lambda is supplied — a send-only service omits it). Each side requires `BootstrapServers`, `SaslUsername` and `SaslPassword` (startup fails fast without them); everything else is a curated tuning override that falls back to a default when unset. A few operational defaults are worth knowing before deploying:

- **`SecurityProtocol` `SaslSsl`** — the SASL credentials are only sent under a SASL protocol, over a TLS transport; override to `Plaintext` only against a local broker.
- **`SaslMechanism` `ScramSha512`** — the SASL mechanism both sides authenticate with.
- **`AutoOffsetReset` `Earliest`** (consumer) — where a group starts when it has no stored offset (its first start, or expired offsets): the at-least-once bias, so a brand-new group replays the topic from the beginning rather than skipping everything published before it joined, mirroring RabbitMQ's queue semantics.
- **Static membership — `GroupInstanceId` defaults to the machine name** (as does `ClientId`) — a restart within the session timeout reclaims the consumer's partition assignment with no rebalance. Two instances of the same service must therefore get **distinct** `GroupInstanceId`s (one per container/replica); sharing one fences the members out of the group — a fatal broker error that stops the app. Note a static member does not leave the group on a clean shutdown; eviction is by session timeout.
- **`StartupMaxConcurrency` `8`** (consumer) — how many consumers open their initial broker connection at once. Each handler is its own consumer, so a service hosting many of them would otherwise fire every TLS/SASL handshake in the same instant at startup — enough to overwhelm a small cluster (`ApiVersionRequest` timeouts, transport failures). This staggers them: a consumer connects, joins its group, then frees a slot for the next. Raise it on a cluster that absorbs more concurrent connects.

```json
{
  "Bus": {
    "Producer": {
      "BootstrapServers": "kafka:9092",
      "SaslUsername": "user",
      "SaslPassword": "pass"
    },
    "Consumer": {
      "BootstrapServers": "kafka:9092",
      "SaslUsername": "user",
      "SaslPassword": "pass"
    }
  }
}
```

### Retries and park topics

`retryIntervals` is the retry ladder — one delay per attempt. A `00:00` entry re-produces the delivery to its topic's tail **immediately**, envelope cloned and `RetryCount` incremented; a **positive** delay parks the delivery through the optional `IRetryScheduler` to be re-produced at its time — register [JorgeCostaMacia.Bus.Kafka.Retry.Quartz](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.Retry.Quartz/) to enable it; with no scheduler registered it parks to `.error` as terminal, since it cannot be delayed. `retryExcludeExceptionTypes` skips the ladder for the listed exception types (inheritance-aware). The default is an empty ladder: the first failure parks.

Each topic has two park topics reached by name suffix, born lazily on the first park (the broker auto-creates them on first produce). A terminal handling failure parks to **`{topic}.error`** as a typed error record — the original message fully typed, the failure stamped in the parked headers (exception type/message, the failing consumer group, the UTC time) with the original envelope cloned alongside; a malformed delivery — undeserializable body, unreadable envelope — and a broken error lane park the raw delivery to **`{topic}.fault`**. Both park headers are filterable and reinjectable, so a drained `.error`/`.fault` topic can be replayed onto the source topic.

Treat a park topic as **exactly as sensitive as the topic it shadows**: a `.error` record carries the original message body in full — the very same domain data as the primary topic — plus the failure detail (exception type, message, stack trace, the inner-exception chain, and any `Exception.Data`), and a `.fault` record carries the raw delivery verbatim. So `{topic}.error` and `{topic}.fault` hold the same payloads as their source, sensitive ones included. Provision them with the **same broker ACLs and retention** as the topic they shadow; never let the park topics pool into a broadly-readable "everything" lane that widens who can read those payloads. And since `Exception.Data` is copied into the parked record, handler authors should never attach a secret (token, credential, patient identifier) to it.

### Topology

Kafka topics are infrastructure, not something the bus declares at startup: both sides run with `AllowAutoCreateTopics` `true`, so a topic — and its `.error` / `.fault` park topics — is born on first use with the **broker's own defaults** (partitions, replication, min-ISR) and is managed broker-side thereafter. The producer connects lazily on first send; each consumer runs one sequential loop on its topic, and scaling out is just running more app instances — the consumer group rebalances the partitions across them (incrementally, via the `CooperativeSticky` default). For anything beyond the broker defaults — partition count, replication factor, retention — provision the topics with your own tooling; the bus does not own their shape.

## Guarantees & consumer contract

Delivery is **at-least-once and unordered** by design, not by accident of tuning — the four points below are the contract every consuming service must hold to. They are not tuning knobs to relax: reading them before writing a handler is the difference between a correct consumer and a subtly broken one.

- **At-least-once — handlers must be idempotent.** The same message can arrive more than once: a crash or a consumer-group rebalance between the handler finishing and its offset being stored replays the last uncommitted delivery on restart. A handler must therefore tolerate reprocessing the same message with **no duplicated side effect** — deduplicate by the message id, or make the write naturally idempotent (upsert on a domain key, guard on the current state) so a second delivery is a no-op.
- **No per-entity ordering.** Messages are produced with a **null Kafka key** — a uniform spread across the topic's partitions, chosen on purpose to keep any one partition from becoming a hot spot — so no order is guaranteed across partitions, nor across the instances of a scaled-out group. Never assume the order two messages arrive in: reconcile by the domain event-time (`AggregateOccurredAt`, last-writer-wins) so a stale or reprocessed message can never overwrite a newer state. A causal chain stays ordered on its own — a follow-up message is only produced once its cause has been handled — so this bites only messages that are genuinely concurrent.
- **Messages must be small — use the claim-check pattern.** The size cap is the standard **1 MB**, and an oversized message is rejected at produce time. Messages carry structured domain data, not payloads — a text invoice PDF is ~5–80 KB in Base64, comfortably within the cap, but a scan, a high-resolution image or any file is not. Store large blobs externally (blob storage, the database) and send a small reference — the *claim-check* — in the message; the consumer fetches the blob only if it needs it. The bus is a data channel, not a file transfer.
- **Batch `Send`/`Publish` is not atomic.** A batch is produced concurrently, message by message; if one fails the others may already be on their topics, and because the delivery that triggered the batch is then retried, the **whole batch is re-produced** on redelivery — so some of its messages are sent more than once. This is the at-least-once model showing through, not a defect: it is exactly why the first point holds — consumers must be idempotent.

## Wire format

Message bodies are JSON serialized with .NET's Web defaults — **camelCase** property names, case-insensitive reads. The envelope travels in **`jcm-`-prefixed headers** (`jcm-message-id`, `jcm-retry-count`, …), hyphenated like the header conventions of HTTP and AMQP; dictionary keys in bodies are user data and travel untouched. On Kafka the header values stay **raw bytes**, since its header API is natively binary; the typed materialization (GUID, `int`, date) happens at the reading boundary on the incoming delivery.

## Logging

Log messages are fixed, low-cardinality grouping keys; every variable detail — topic, group id, body, the decoded envelope headers, the `BusDescription` outcome expansion — travels as structured properties through Serilog's `LogContext`. Wire `.Enrich.FromLogContext()` into the host's Serilog pipeline (the usual default) so those properties reach the sinks.

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Brings [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/) (the transport-agnostic contracts) transitively, plus [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/).

## About

`JorgeCostaMacia.Bus.Kafka` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
