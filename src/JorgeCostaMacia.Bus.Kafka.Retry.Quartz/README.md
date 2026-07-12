# JorgeCostaMacia.Bus.Kafka.Retry.Quartz

**Quartz-backed delayed retry for `JorgeCostaMacia.Bus.Kafka`** — a persistent, clustered `IRetryScheduler` that parks a failed delivery as a durable [Quartz](https://www.nuget.org/packages/Quartz/) job and re-produces it to its topic when the delay elapses — retrying every five minutes while the produce keeps failing, then staying parked as a dead-letter. The bus's immediate retries (a `00:00` interval) requeue through Kafka; the **positive** intervals of the retry ladder go through this scheduler. Register the scheduler on the **sending** service; the **worker fleet** just runs Quartz against the shared store.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.Kafka.Retry.Quartz.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.Retry.Quartz/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.Kafka.Retry.Quartz.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.Retry.Quartz/)
[![Build](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-net/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-net.svg)](https://github.com/JorgeCostaMacia/bus-net/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.Kafka.Retry.Quartz
```

## Usage

This package is **agnostic to the Quartz store** — you configure Quartz (its store, clustering and serialization) as usual; it only plugs the retry `IRetryScheduler` and its job onto that scheduler. The store must be **persistent and shared** between the sending service and the worker fleet: scheduling commits to the store *before* the delivery is acked, so an in-memory store would drop the retry on a restart.

**Sending service** — parks delayed retries as Quartz jobs:

```csharp
services
    .AddBusContext(configuration, producer => producer.AddCommand<PlaceOrder>("orders"), consumer => consumer.AddCommandHandler<PlaceOrder, PlaceOrderHandler>("orders.handler"))
    .AddQuartz(q => q.UsePersistentStore(store =>
    {
        store.UsePostgres(/* connection string */);   // any provider
        store.UseProperties = true;                    // string-only job data
        store.UseClustering();
        store.UseSystemTextJsonSerializer();
    }))
    .AddRetryContext();                                // registers the Quartz-backed IRetryScheduler
```

**Worker fleet** — runs Quartz against the *same* store; fires the jobs and re-produces the message. It needs the bus producer registered so the job can produce; no extra retry registration — Quartz's DI job factory resolves the job on its own:

```csharp
services
    .AddBusContext(configuration, producer => producer.AddCommand<PlaceOrder>("orders"))
    .AddQuartz(q => q.UsePersistentStore(/* the same store */))
    .AddQuartzHostedService();
```

Each parked retry is a **durable** job grouped under its topic, named `messageId:retryCount` (traceable; the same delivery parked twice just overwrites itself) and described with the failing consumer group id. Its single trigger fires the produce **exactly at the scheduled time**, then repeats it **every five minutes** while it keeps failing — four re-executions after the first fire, the same semantics as the bus's retries. A successful produce deletes the job; with the attempts exhausted Quartz completes the trigger and the durable job stays **parked as the dead-letter**: trigger-less, visible in the store, re-firable with `IScheduler.TriggerJob` (it deletes itself when a re-fire finally succeeds). The produce exception bubbles out of the job, so any Quartz job listeners (e.g. `JorgeCostaMacia.Quartz.Serilog`) observe every failed attempt.

### Finding and replaying dead-letters

A dead-letter is a **durable job with no trigger** left in the store. List them straight from the Quartz tables (Postgres, default prefix):

```sql
SELECT jd.job_name, jd.job_group, jd.description   -- messageId:retryCount, topic, failing group id
FROM qrtz_job_details jd
LEFT JOIN qrtz_triggers t
  ON  t.sched_name = jd.sched_name
  AND t.job_name  = jd.job_name
  AND t.job_group = jd.job_group
WHERE jd.is_durable = true
  AND t.trigger_name IS NULL;                       -- no trigger => the retry ladder is exhausted
```

Re-fire one to retry the produce (it deletes itself once the produce finally succeeds):

```csharp
await scheduler.TriggerJob(new JobKey(jobName, jobGroup), cancellationToken);
```

## Requirements

One of the following SDKs: **.NET 8 / 9 / 10** *(.NET 10 recommended)*.

Brings [JorgeCostaMacia.Bus.Kafka](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/) (the bus over Kafka) and [Quartz](https://www.nuget.org/packages/Quartz/) transitively.

## About

`JorgeCostaMacia.Bus.Kafka.Retry.Quartz` is part of **[bus-net](https://github.com/JorgeCostaMacia/bus-net)** — messaging building blocks, each scoped to a single concern.

- **Repository:** [github.com/JorgeCostaMacia/bus-net](https://github.com/JorgeCostaMacia/bus-net)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-net/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-net/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-net/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
