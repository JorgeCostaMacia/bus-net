# JorgeCostaMacia.Bus.Command

**Command bus contracts** — the CQRS command side of `JorgeCostaMacia.Bus`: `ICommand`, the `Command` base record, a command-only `ICommandBus` and `ICommandHandler`. Point-to-point: one command, one handler. Transport-agnostic — the RabbitMQ/Kafka implementations live in sibling packages.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Command.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Command/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Command.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Command/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Command
```

## Contracts

| Type | For |
| --- | --- |
| `ICommand` | marker — a command (`: ITracedMessage, IFilteredMessage`) |
| `Command` | `abstract record` base: id / correlation / UTC time / addresses, id defaulted via GuidFactory |
| `ICommandBus` | `: ISenderBus<ICommand>` — sends commands point-to-point (compiler enforces command-only) |
| `ICommandContext<TCommand, TTransport>` | the command handler context — `Command` + `Transport` (`TTransport : ITransport`) plus the typed envelope facets |
| `ICommandHandler` / `ICommandHandler<TCommand, TContext>` | handle a command with the context shape it needs |

```csharp
public sealed record CreateOrderCommand(Guid OrderId)
    : Command(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateDestinationAddresses: null);
```

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Depends on [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/) and [JorgeCostaMacia.GuidFactory](https://www.nuget.org/packages/JorgeCostaMacia.GuidFactory/).

## About

`JorgeCostaMacia.Bus.Command` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
