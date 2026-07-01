# JorgeCostaMacia.Bus.UrnFactory

**URN factory for `JorgeCostaMacia.Bus`** — builds the ordered `MessageTypeUrn` list from a message type: its own URN plus the URNs of its (non-system) interfaces and base types, as `urn:message:{FullTypeName}`. Gives every message a hierarchical identity for **polymorphic routing** and **versioning** (e.g. subscribing to `IEvent` to receive every domain event). Shared by the transports so they build the URN list the same way.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.UrnFactory.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.UrnFactory/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.UrnFactory.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.UrnFactory/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.UrnFactory
```

## Usage

```csharp
ImmutableList<string> urns = UrnFactory.Create<OrderPlaced>();
// urn:message:MyApp.OrderPlaced, urn:message:…IEvent, …  (ordered by length)
```

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

## About

`JorgeCostaMacia.Bus.UrnFactory` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
