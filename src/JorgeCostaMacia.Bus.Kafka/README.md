# JorgeCostaMacia.Bus.Kafka

**Kafka transport for `JorgeCostaMacia.Bus`** — the concrete command/event bus over [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/): the `Command`/`Event` base records, the `Transport`, the `CommandContext<T>`/`EventContext<T>`, the ergonomic `CommandHandler<T>`/`EventSubscriber<T>` bases, and the producer/consumer wiring. **Reference this one package** to run the bus on Kafka — it brings the `JorgeCostaMacia.Bus.*` contracts transitively.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Kafka.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Kafka.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Kafka
```

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
