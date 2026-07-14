<p align="center">
  <img src="https://raw.githubusercontent.com/JorgeCostaMacia/bus-net/main/assets/social-preview.png" width="100%" alt="bus-net" />
</p>

# bus-net

> Messaging building blocks for .NET — CQRS command and event buses and message abstractions over pluggable transports (RabbitMQ, Kafka) — each scoped to a single concern and shipped independently under `JorgeCostaMacia.Bus.*`.

[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](LICENSE.txt)
[![Main](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![Develop](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/develop.yml/badge.svg?branch=develop)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/develop.yml)

Part of the `JorgeCostaMacia.*` family, on top of the [shared-net](https://github.com/JorgeCostaMacia/shared-net) DDD foundation, alongside [http-net](https://github.com/JorgeCostaMacia/http-net) (ASP.NET Core).

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

## Packages

A MassTransit-free bus — one package per concern:

| Package | What it does |
| --- | --- |
| `JorgeCostaMacia.Bus` | Core message + bus contracts (no request/response). |
| `JorgeCostaMacia.Bus.RabbitMQ` | Transport on the official `RabbitMQ.Client` — commands (`Send`) and events (`Publish`). |
| `JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz` | Quartz-backed delayed retry for the RabbitMQ transport — parks a failed delivery and re-produces it at its time. |
| `JorgeCostaMacia.Bus.RabbitMQ.HealthChecks` | Health check for the RabbitMQ transport — the shared connection's state on ASP.NET Core health endpoints. |
| `JorgeCostaMacia.Bus.Kafka` | Transport on `Confluent.Kafka` — commands (`Send`) and events (`Publish`). |
| `JorgeCostaMacia.Bus.Kafka.Retry.Quartz` | Quartz-backed delayed retry for the Kafka transport — parks a failed delivery and re-produces it at its time. |
| `JorgeCostaMacia.Bus.Kafka.HealthChecks` | Health check for the Kafka transport — the bus's broker reachability on ASP.NET Core health endpoints. |

## Contact

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
