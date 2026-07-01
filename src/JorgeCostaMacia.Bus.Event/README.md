# JorgeCostaMacia.Bus.Event

**Event bus contracts** — the pub/sub event side of `JorgeCostaMacia.Bus`: the `IEvent` marker (a domain event that travels on the bus), an event-only `IEventBus`, `IEventContext` and `IEventSubscriber`. Pub/sub: one event, many subscribers. **Interface-only working contract** — the concrete `Event` base record, context and subscriber bases live in each transport (RabbitMQ/Kafka), co-located so a dev finds everything for "events on transport X" in one place.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Event.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Event/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Event.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Event/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Event
```

## Contracts

| Type | For |
| --- | --- |
| `IEvent` | marker — a domain event on the bus (`: IDomainEvent, ITracedMessage, IFilteredMessage`) |
| `IEventBus` | `: IPublisherBus<IEvent>, IPublisherTracedBus<IEvent>` — publishes events pub/sub, plain or correlated |
| `IEventContext` / `IEventContext<TEvent, TTransport>` | the subscriber context — the glue: composes `IMessageContext<TEvent>` + `ITransportContext<TTransport>` + the envelope facets |
| `IEventSubscriber` / `IEventSubscriber<TEvent, TContext, TTransport>` | subscribe to an event with the exact context shape it needs |

The concrete `Event` base record and the ergonomic `EventContext<T>` / `EventSubscriber<T>` bases live in each transport, so an end subscriber declares only its event type:

```csharp
// in your service, over a transport package (e.g. Kafka):
public sealed record OrderPlaced(Guid OrderId) : Event(/* id … */);

public sealed class OrderPlacedSubscriber : EventSubscriber<OrderPlaced>
{
    public override Task Handle(EventContext<OrderPlaced> ctx, CancellationToken ct = default) { /* … */ }
}
```

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Depends on [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/) and [JorgeCostaMacia.DomainEvent](https://www.nuget.org/packages/JorgeCostaMacia.DomainEvent/).

## About

`JorgeCostaMacia.Bus.Event` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
