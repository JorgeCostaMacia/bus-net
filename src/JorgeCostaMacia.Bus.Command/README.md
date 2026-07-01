# JorgeCostaMacia.Bus.Command

**Command bus contracts** — the CQRS command side of `JorgeCostaMacia.Bus`: the `ICommand` marker, a command-only `ICommandBus`, `ICommandContext` and `ICommandHandler`. Point-to-point: one command, one handler. **Interface-only working contract** — the concrete `Command` base record, context and handler bases live in each transport (RabbitMQ/Kafka), co-located so a dev finds everything for "commands on transport X" in one place.

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
| `ICommandBus` | `: ISenderBus<ICommand>, ISenderTracedBus<ICommand>` — sends commands point-to-point, plain or correlated (compiler enforces command-only) |
| `ICommandContext` / `ICommandContext<TCommand, TTransport>` | the command handler context — the glue: composes `IMessageContext<TCommand>` + `ITransportContext<TTransport>` + the envelope facets |
| `ICommandHandler` / `ICommandHandler<TCommand, TContext, TTransport>` | handle a command with the exact context shape it needs |

The concrete `Command` base record and the ergonomic `CommandContext<T>` / `CommandHandler<T>` bases live in each transport, so an end handler declares only its command type:

```csharp
// in your service, over a transport package (e.g. Kafka):
public sealed record CreateOrderCommand(Guid OrderId) : Command(/* id … */);

public sealed class CreateOrderHandler : CommandHandler<CreateOrderCommand>
{
    public override Task Handle(CommandContext<CreateOrderCommand> ctx, CancellationToken ct = default) { /* … */ }
}
```

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Depends on [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/).

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
