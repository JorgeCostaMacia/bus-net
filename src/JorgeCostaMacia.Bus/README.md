# JorgeCostaMacia.Bus

**Transport-agnostic messaging contracts** — the root of the `JorgeCostaMacia.Bus.*` family: messages, read-only message contexts (composable envelope facets) and point-to-point / pub-sub buses. No transport, no framework — the RabbitMQ and Kafka implementations live in sibling packages.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus
```

## Contracts

| Type | For |
| --- | --- |
| `IMessage` | root marker for anything on the bus |
| `ITracedMessage` | + `AggregateId` / `AggregateCorrelationId` / `AggregateOccurredAt` |
| `IFilteredMessage` | + `AggregateConsumers` (consumer-side filtering) |
| `IContext` | marker for the read-only envelope a handler receives around a delivered message |
| `ITransport` | marker for the transport a message arrived on (a Kafka/RabbitMQ transport object; one of the two real objects of a delivery, alongside the message) |
| context facets (each carries only what it needs) | `IMessageContext<T>` (the message) · `ITransportContext<TTransport>` (the transport) · `ITracedContext` (messaging trace: id/type/URNs/addresses) · `IAggregateTracedContext` (domain trace) · `IAggregateFilteredContext` (addresses) · `IConversationContext` · `IResilientContext` · `IHostContext` (the handling host) · `IErrorContext<TError>` (a parked failure) — a concrete context (command/event) composes the ones it exposes |
| `IBus` | marker for a concrete bus — the single entry point a service resolves; the sender/publisher contracts below define it, they are not resolvable ports |
| `ISenderBus<TMessage>` / `ISenderBatchBus<TMessage>` | `Send` point-to-point — one message, or a batch |
| `IPublisherBus<TMessage>` / `IPublisherBatchBus<TMessage>` | `Publish` pub/sub — one message, or a batch |
| `IHandler<TMessage, TContext>` | handle a delivered message (`TContext : IContext`) |
| `ErrorInfo` / `IErrorMessage` | the failure a transport parks with a failed delivery — the whole exception chain modeled (type, message, source, stack trace, inner), so any parked failure reads the same |

Ordering is a non-concern by design: transports partition freely and consumers resolve conflicts by `AggregateOccurredAt` (event-time last-writer-wins).

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

## About

`JorgeCostaMacia.Bus` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern and reusable across your services.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
