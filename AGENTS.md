# bus-net — working in this repo

Messaging building blocks — CQRS **command** and **event** buses and message abstractions over pluggable transports (**RabbitMQ**, **Kafka**), each scoped to a single concern and shipped independently on NuGet under `JorgeCostaMacia.Bus.*`. Part of the `JorgeCostaMacia.*` family, on top of the **shared-net** foundation (consumed as published NuGet packages). **No MassTransit** in the bus itself.

## Layout

- `src/<Package>/` — one package per folder. `test/<Package>.Tests/` — its tests. `assets/` — icons + social preview.
- **3-tier `Directory.Build.props`**: **root** (repo identity + the single lockstep `VersionPrefix`; TFMs `net8.0;net9.0;net10.0`; ImplicitUsings, Nullable, AnalysisLevel, EnforceCodeStyleInBuild) → **`src/`** (package-output: icon / readme / license, SourceLink, symbols, `GenerateDocumentationFile`, pack of LICENSE/COPYRIGHT/icon/README) → **`test/`** (test settings). Each `src` csproj declares **only** `Description` / `PackageTags`; everything else is inherited.

## Architecture — concrete-first transports over agnostic contracts (locked)

```
JorgeCostaMacia.Bus            root: transport- AND pattern-agnostic vocabulary — IMessage / ITracedMessage /
│                              IFilteredMessage, ITransport, IContext + envelope facets, ISenderBus<T> /
│                              IPublisherBus<T>, IHandler (NO requester / request-response, NO ICommand/IEvent)
├─ Bus.RabbitMQ               own implementation on the official RabbitMQ.Client (NOT MassTransit)
└─ Bus.Kafka                  own implementation on Confluent.Kafka
```

- **Concrete-first**: the command/event distinction is defined BY EACH TRANSPORT, not the root — `Bus.Kafka` ships `Command` / `Event` abstract records (implementing the root message contracts; `Event` also `IDomainEvent`), `CommandHandler<T>` / `EventSubscriber<T>` bases (`: IHandler<T, …Context<T>>`) and `IBus : ISenderBus<Command>, IPublisherBus<Event>`. `Bus.RabbitMQ` mirrors the same simple names in its own namespace; migrating transports is swapping a `global using`. Cross-transport shared code types against the root contracts (`ITracedMessage`, facets, `IHandler`), which both transports implement.

- **No requester** — the RabbitMQ-only request/response bus is dropped (it was the only hard-to-port piece). **No query bus** — dropped.
- **Ordering is a non-concern by design**: Kafka partitions on its own (no message key); consumers resolve conflicts by **`ITracedMessage.AggregateOccurredAt`** (event-time last-writer-wins), so out-of-order / reprocessed messages never overwrite a newer applied one. `AggregateId` is internal domain/tracing metadata, **not** a partition key.
- **MassTransit** stays out of the bus. A temporary `Bus.MassTransit.RabbitMq` bridge (apps' current dependency) may be kept until the own transports work — that decision is deferred to the end.

## Dependencies

- **Cross-repo, on shared-net**: the transports → `JorgeCostaMacia.DomainEvent` (their `Event` record implements `IDomainEvent`) — **`PackageReference`** to the published package, pinned in `Directory.Packages.props`. Never `ProjectReference` across repos.
- **Intra-repo, between `Bus.*` packages** (`RabbitMQ`/`Kafka` → `Bus`): **`ProjectReference`** (lockstep; pack emits nuspec `<dependency>` at the shared version).
- **Transport clients**: `RabbitMQ.Client`, `Confluent.Kafka` — third-party `PackageReference`, versioned in `Directory.Packages.props`.

## Dependencies — Central Package Management

Third-party **and** the cross-repo shared-net versions are centralized in **`Directory.Packages.props`** (`ManagePackageVersionsCentrally=true`): add/bump as `<PackageVersion>`, reference in csproj **without** a `Version`. Intra-repo `Bus.*` deps are `ProjectReference`, not managed by CPM.

## Versioning — lockstep

A single **`<VersionPrefix>`** lives in the **root `Directory.Build.props`** — bump once, everything moves together. Never put `VersionPrefix` back in individual csproj.

## CI / publishing

- `main.yml`: push to `main` → build → test → `dotnet pack bus-net.slnx` → `dotnet nuget push` (nuget.org via Trusted Publishing / OIDC). **The central `VersionPrefix` is the publish gate.**
- `develop.yml`: build/test on develop + PRs (no publish).
- `release.yml`: on a pushed `v*` tag → creates the GitHub Release.
- All three declare **top-level** `permissions:`.

## Branching & releases — GitFlow

Use the **`gitflow` skill** for any branch/release work.

- Feature/bugfix → `feature/`|`bugfix/<name>-<ts>` from develop → finish `--no-ff` into develop.
- Release → `release/<version>` from develop → bump the **single** `VersionPrefix` in the **root** props → Release Finish (merge develop+main, annotated tag `v<version>`, atomic push).
- git's **default merge message** (`--no-ff --no-edit`, never `-m`). Branch prefixes only: `feature` / `bugfix` / `release` / `hotfix`.

## Git etiquette

- Commit under **your own identity** — don't hardcode anyone's name/email.
- Keep history clean — **no** `Co-Authored-By` / AI-assistant trailers.
- Merges use git's **default** message.

## Relevant skills

Skills that apply to this repo — let them trigger, or invoke explicitly. `gitflow`, `solid`, `clean-architecture`, `ddd`, `testing` and `logging-net` are from `jorgecostamacia-agent-skills`; the rest from `dotnet-agent-skills` (the `dotnet/skills` marketplace).

- **`gitflow`** — all branch/release work.
- **`solid`** — SOLID-principles design review; apply when shaping or reviewing the public surface (bus interfaces, worker/handler seams, transport options).
- **`clean-architecture`** — layers and the inward dependency rule; these packages are the messaging Infrastructure/Presentation seam (bus consumers ARE driving adapters) consumed by the bounded contexts.
- **`ddd`** — tactical DDD; here mainly **domain events vs integration events** (the `IDomainEvent` marker crossing into transport contracts is this repo's core concept) and typed domain errors.
- **`testing`** — testing principles: done-means-tested, one test file per unit, names as specification, classicist doubles (the transport fakes), rule coverage.
- **`logging-net`** — the logging style for every log statement: fixed low-cardinality messages as grouping keys, all variable data via the log context, correlation ids in every scope (the bus's `BusLogger` + description-context pattern follows it).
- **`dotnet`** — C# language server + general .NET development (the transport implementations live here).
- **`dotnet-msbuild`** — `Directory.Build.props`, project-file quality, CPM.
- **`dotnet-nuget`** — dependency management.
- **`dotnet-test`** / **`dotnet-test-migration`** — tests; the xUnit.v3 / MTP setup.
- **`dotnet-upgrade`** — target-framework migrations.

Not relevant to this repo (skip): `validation-net` (no FluentValidation here — messages are primitive DTO contracts; boundary validation is the consuming app's job), `dotnet-aspnetcore` (that's http-net), `dotnet-ai`, `dotnet-maui`, `dotnet-blazor`, `dotnet-data`, `dotnet-template-engine`, `dotnet11`, `dotnet-diag`, `dotnet-advanced`.

## Build & test

```
dotnet format bus-net.slnx                  # apply .editorconfig (using order, whitespace) — run before committing
dotnet build  bus-net.slnx -c Release
dotnet test   bus-net.slnx -c Release       # MTP prints a per-assembly summary; --logger is VSTest-only (MTP0001)
dotnet pack   bus-net.slnx -c Release        # packs all packable; tests are IsPackable=false
```

Run **`dotnet format` before committing** — it applies the `.editorconfig` (using ordering, whitespace), the CLI equivalent of Visual Studio's *Code Cleanup*, so generated code doesn't drift from what the IDE would produce.
