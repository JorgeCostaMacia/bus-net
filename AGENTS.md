# bus-net — working in this repo

Messaging building blocks — CQRS **command** and **event** buses and message abstractions over pluggable transports (**RabbitMQ**, **Kafka**), each scoped to a single concern and shipped independently on NuGet under `JorgeCostaMacia.Bus.*`. Part of the `JorgeCostaMacia.*` family, on top of the **shared-net** foundation (consumed as published NuGet packages). **No MassTransit** in the bus itself.

## Layout

- `src/<Package>/` — one package per folder. `test/<Package>.Tests/` — its tests. `assets/` — icons + social preview.
- **Internal visibility**: each project grants `InternalsVisibleTo` to its matching `.Tests`; the transports additionally grant it to their own satellite packages (`*.HealthChecks`, `*.Retry.Quartz`, and each satellite's `.Tests`) — deliberate, since those satellites extend the transport's internal seam and version in lockstep with it.
- **3-tier `Directory.Build.props`**: **root** (repo identity + the single lockstep `VersionPrefix`; TFM `net10.0`; ImplicitUsings, Nullable, AnalysisLevel, EnforceCodeStyleInBuild, TreatWarningsAsErrors, the NuGet audit; `IsPackable` false by default) → **`src/`** (`IsPackable` true, package-output: icon / readme / license, SourceLink, symbols, `GenerateDocumentationFile`, pack of LICENSE/COPYRIGHT/icon/README) → **`test/`** (test settings). Each `src` csproj declares **only** `Description` / `PackageTags`; everything else is inherited.

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
- **Ordering is a non-concern by design**: Kafka partitions on its own (no message key); consumers resolve conflicts by a version their data carries, applied only when strictly newer (see the `messaging` skill), so out-of-order / reprocessed messages never overwrite a newer applied one. `AggregateOccurredAt` is when a message was produced, not the version of what it describes. `AggregateId` is internal domain/tracing metadata, **not** a partition key.
- **MassTransit** stays out of the bus. A temporary `Bus.MassTransit.RabbitMq` bridge (apps' current dependency) may be kept until the own transports work — that decision is deferred to the end.

## Dependencies

- **Cross-repo, on shared-net**: the transports → `JorgeCostaMacia.DomainEvent` (their `Event` record implements `IDomainEvent`) — **`PackageReference`** to the published package, pinned in `Directory.Packages.props`. Never `ProjectReference` across repos.
- **Intra-repo, between `Bus.*` packages** (`RabbitMQ`/`Kafka` → `Bus`): **`ProjectReference`** (lockstep; pack emits nuspec `<dependency>` at the shared version).
- **Transport clients**: `RabbitMQ.Client`, `Confluent.Kafka` — third-party `PackageReference`, versioned in `Directory.Packages.props`.
- **Quartz 4** (`Quartz*` 4.6.2) — this upgrade is what forced the single `net10.0` target: **Quartz 4 ships only `lib/net10.0`** (3.20 shipped net462/net472/netstandard2.0/net8/net9/net10), so multi-targeting and Quartz 4 are mutually exclusive. Its breaking changes as they land here: `IJob.Execute` takes a `CancellationToken` and returns `ValueTask`; `ScheduleJob` / `AddJob` replaced the `replace` bool with `ScheduleJobOptions` / `AddJobOptions` (`…Options.Replacing`); the matchers moved from `Quartz.Impl.Matchers` to `Quartz`; `StdSchedulerFactory` is gone, replaced by `QuartzSchedulerBuilder.Create().UseProperties(…).Build()`; `IPersistentStoreBuilder.UseProperties` became `ConfigureStore(options => options.StoreJobDataAsStrings = true)`; and `IJobExecutionContext` lost `Get` / `Put` (use `MergedJobDataMap`) and gained `RetryAttempt`.
- **The Quartz 4 schema is validated on start and is not the 3.x one** — it adds `QRTZ_PAUSED_JOB_GRPS` and new columns (`RETRY_POLICY`, `RETRY_ATTEMPT`, `PREFERRED_NODE`), so a 3.x schema fails `ValidateSchema` at startup. Quartz 4 embeds the DDL per provider, so the integration fixtures no longer carry a hand-written copy of `tables_postgres.sql`: the test store calls **`store.ProvisionSchema()`** and Quartz creates whatever is missing. It only ever *creates* — it is not an upgrade, so a **live** 3.x database still needs `database/migrations/4.0/schema_30_to_40_upgrade_postgres.sql` run before the fleet starts on Quartz 4. Quartz 4.6 validates three more nullable columns (`SCHEDULER_STATE.QUARTZ_VERSION`, `FIRED_TRIGGERS.INTERRUPT_REQUESTED_TIME`, `TRIGGERS.MISFIRE_THRESHOLD`, from `database/migrations/4.6`): a live store is migrated before any 4.6 node starts, a worker that only parks retries included, since parking writes `TRIGGERS`. A 4.5 node never names them, so the migration can run first and the nodes roll after. 4.6.1 and 4.6.2 change no API and no schema; since 4.6.1 a scheduler start no longer resets a declared trigger's fire count, a retry pending at shutdown or a pause an operator set.

## Dependencies — Central Package Management

Third-party **and** the cross-repo shared-net versions are centralized in **`Directory.Packages.props`** (`ManagePackageVersionsCentrally=true`): add/bump as `<PackageVersion>`, reference in csproj **without** a `Version`. Intra-repo `Bus.*` deps are `ProjectReference`, not managed by CPM.

**The strict gates are on**: `TreatWarningsAsErrors` and the NuGet vulnerability audit on every restore (direct and transitive, every severity), so any warning — an advisory included — fails the build. The fix is a version bump or a transitive override in CPM, never `NoWarn`. A third-party version lands once it is three days old, a security fix the day it ships (see `build-net`). The root `nuget.config` clears every inherited source and keeps nuget.org alone: this library consumes nothing else, and an inherited second source would raise `NU1507`. It is **deny-read** for agents — hand changes to it to the user.

## Versioning — lockstep

A single **`<VersionPrefix>`** lives in the **root `Directory.Build.props`** — bump once, everything moves together. Never put `VersionPrefix` back in individual csproj.

## Code style

Source is **UTF-8 without BOM**; camelCase locals, PascalCase types, I-prefixed interfaces. **Explicit types everywhere — spell the type out.** Never `var`, never target-typed `new()`, never collection expressions `[]`: write `new Foo(...)`, `new byte[] { ... }`, `new List<T> { ... }`, `Array.Empty<T>()`. The `.editorconfig` sets all three to explicit, but only `var` is analyzer-enforced — `new()`/`[]` can't be flagged (the analyzer never reports the implicit form), so `develop.yml` guards target-typed `new(` and implicit `new[]` with a **grep step** that fails the build; a collection expression `[…]` has no guard and is caught in review only. Lists follow the 120-column rule of `code-style-net` — declarations and constructions vertical, long calls and comments wrapped. Braces are **always** required — a single-line `if` body still gets braces (this one IS analyzer-enforced, IDE0011). Do not introduce `var` / `new()` / `[]` / brace-less bodies when editing.

## CI / publishing

- `main.yml`: push to `main` → build → test → `dotnet pack bus-net.slnx` → `dotnet nuget push` (nuget.org via Trusted Publishing / OIDC). **The central `VersionPrefix` is the publish gate.**
- `develop.yml`: on develop + PRs (no publish): the explicit-types grep, build, the `dotnet format` check, test, and pack as a packaging check. `.gitattributes` checks `*.cs` out as CRLF, so the Linux runner sees what the `.editorconfig` requires.
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

Skills that apply to this repo — let them trigger, or invoke explicitly. The first group is from `jorgecostamacia-agent-skills` (each `-net` skill brings the generic one it realizes); the rest from `dotnet-agent-skills` (the `dotnet/skills` marketplace).

- **`bus-net`** (with **`messaging`**) — how services use this library: the `Command`/`Event` records and their two constructors, thin handlers and subscribers, `Send`/`Publish` with the incoming transport, the retry ladder with `ValidationException` excluded, retries parked in Quartz and run by a separate jobs worker, the route and handler naming, the producer/admin/consumer maps. `messaging` is the why — duplicates and disorder, the version travelling with the data. A change here is a change in what that skill describes: keep the two in step.
- **`bus-kafka-net`** — the decisions on top of the Kafka transport: the topic is the route with the contracts' major, partitions per service, no message key on purpose, the consumer group named after the handler, parked messages in `.error`.
- **`logging-net`** (with **`logging`**) — the logging style the bus's `BusLogger` and description-context pattern follow: fixed messages, every variable value through the log context, correlation in every scope.
- **`code-style-net`** — how the C# is written: explicit types, the 120-column width with declarations and constructions vertical, comments wrapped too, the gates.
- **`build-net`** — the three-tier `Directory.Build.props`, the single `VersionPrefix`, CPM, the three-day package age, the NuGet audit as a build gate, the library's packing metadata, and the `nuget.config` base.
- **`testing-net`** (with **`testing`**) — xUnit v3 on Microsoft.Testing.Platform, the test project layout, `TestContext.Current.CancellationToken`, Testcontainers fixtures (Kafka, RabbitMQ, Postgres).
- **`ddd`** — domain events and typed domain errors: the `IDomainEvent` marker crossing into the transport contracts is this repo's core concept.
- **`clean-architecture`** / **`solid`** — the bus as the messaging Infrastructure/Presentation seam (consumers ARE driving adapters), and design review of the public surface.
- **`gitflow`** — all branch/release work.
- **`dotnet`** — C# language server + general .NET development (the transport implementations live here).
- **`dotnet-msbuild`** — `Directory.Build.props`, project-file quality, CPM.
- **`dotnet-nuget`** — dependency management.
- **`dotnet-test`** / **`dotnet-test-migration`** — tests; the xUnit v4 / MTP v2 setup.
- **`dotnet-upgrade`** — target-framework migrations.

Not relevant to this repo (skip): `validation-net`, `ddd-net`, `efcore-net` (no FluentValidation, value objects or EF here — messages are primitive DTO contracts; boundary validation is the consuming app's job), `dotnet-aspnetcore` (that's http-net), `dotnet-ai`, `dotnet-maui`, `dotnet-blazor`, `dotnet-data`, `dotnet-template-engine`, `dotnet11`, `dotnet-diag`, `dotnet-advanced`.

## Build & test

```
dotnet format bus-net.slnx                  # apply .editorconfig (using order, whitespace) — run before committing
dotnet build  bus-net.slnx -c Release
dotnet test   bus-net.slnx -c Release       # MTP v2 via global.json (needs the .NET 10 SDK); --logger is VSTest-only (MTP0001)
dotnet pack   bus-net.slnx -c Release        # packs all packable; tests are IsPackable=false
```

Tests are **xUnit v4 on Microsoft.Testing.Platform v2** — test projects are `OutputType=Exe`, and `dotnet test` runs MTP because the root **`global.json`** opts in (`"test": { "runner": "Microsoft.Testing.Platform" }`). MTP v2 dropped the VSTest bridge, so `TestingPlatformDotnetTestSupport` is gone and running the tests needs the **.NET 10 SDK**. The integration suites (Kafka / RabbitMQ / Postgres via Testcontainers) are the slow ones: running the whole solution starts the four of them at once (one per project, now that the repo single-targets) and Docker contention can make them flake — re-run a failing suite on its own (its MTP `.exe` under `bin/Release/net10.0/`) before treating it as a real failure.

Run **`dotnet format` before committing** — it applies the `.editorconfig` (using ordering, whitespace, line endings), the CLI equivalent of Visual Studio's *Code Cleanup*, and it is the normalizer for a `.cs` a shell tool wrote with LF. Develop checks it after the build with `--verify-no-changes`: the solution is clean, so the check only ever fails on what the push itself brought.
