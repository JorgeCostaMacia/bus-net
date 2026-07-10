# JorgeCostaMacia.Bus.Kafka.HealthChecks

**Health check for `JorgeCostaMacia.Bus.Kafka`** — reports whether the bus reaches the Kafka brokers, for ASP.NET Core health endpoints (liveness/readiness). No probe connection, no extra I/O: the check reads a reachability tracker the transport already feeds — the client's *all brokers down* signal flips it down, any successful produce or consumed delivery flips it back up. A **separate package** so the transport stays free of the HealthChecks dependency.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Kafka.HealthChecks.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.HealthChecks/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Kafka.HealthChecks.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.HealthChecks/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Kafka.HealthChecks
```

## Usage

One `AddKafkaBus` call plugs the check onto the standard health-check pipeline — after `AddBusContext`, which registers the reachability tracker the check reads. Tag it (typically `"ready"`) to pick which health endpoints run it.

```csharp
services.AddBusContext(configuration,
    producer => producer.AddCommand<PlaceOrder>("orders"));

services.AddHealthChecks().AddKafkaBus(tags: ["ready"]);
```

The check reports:

- **Healthy** — the bus reaches the brokers, *or nothing has been observed yet*: an untouched bus counts as reachable, so a service that has not produced or consumed is not a failure.
- **The registration's failure status** (`Unhealthy` by default; pass `failureStatus` to soften it) — the client reported **every** broker down (librdkafka's *all brokers down*); the client keeps reconnecting on its own, and the first successful produce or consumed delivery flips the check back to healthy. The result's data carries the UTC instant of the flip under `changedAt`, so the endpoint shows how long the outage has held.

A single failed produce does **not** flip the check — only the client's whole-broker-set signal does; per-message failures belong to the bus's retry and parking lanes.

The check registers under the name `bus-kafka` (override it with `name` when one service runs several buses).

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Brings [JorgeCostaMacia.Bus.Kafka](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/) (the bus over Kafka) and [Microsoft.Extensions.Diagnostics.HealthChecks](https://www.nuget.org/packages/Microsoft.Extensions.Diagnostics.HealthChecks/) transitively.

## About

`JorgeCostaMacia.Bus.Kafka.HealthChecks` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
