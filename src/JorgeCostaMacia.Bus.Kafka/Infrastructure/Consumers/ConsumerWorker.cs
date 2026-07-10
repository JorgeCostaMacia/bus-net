using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

/// <summary>
/// Base for the hosted consumers — one per handler, one consumer loop on its topic (scale out by
/// running more app instances; the consumer group balances the partitions). The loop is strictly
/// sequential: a handler outrunning <c>MaxPollIntervalMs</c> (5 minutes by default) evicts the
/// consumer from the group until the next consume rejoins — raise that knob for slower handlers. Its
/// Kafka configuration arrives as a ready-made <see cref="Domain.IConsumer"/> gate (settings and
/// logging handlers wired where it is composed); its contract — topic, group id — through the
/// constructor.
/// The whole delivery flow lives here once: consume → filter → open one service scope for the
/// delivery → rebuild transport/context → handle → store the offset (the store is the ack; the
/// background thread commits it without blocking). The handler, its error handler and its fault
/// handler are all resolved from that per-delivery scope, so each failure gets a clean handler that
/// carries no state across deliveries. A handler failure is handed to the message's error handler,
/// which reports through <see cref="ErrorResult"/> whether it requeued, scheduled or parked; a
/// malformed delivery — or an error handler that reports <see cref="ErrorResult.Faulted"/> — is
/// handed to the fault handler. The concrete consumers plug in only what differs — building their
/// context, invoking their handler, resolving their error and fault handlers from the scope, and the
/// pub/sub-only consumer-side filtering. On shutdown the loop is cancelled and the consumer closed
/// gracefully.
/// </summary>
/// <typeparam name="TContext">The context type delivered to the handler.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where TContext : IContext
    where THandler : IHandler
{
    /// <summary>The consumer group id — the consumer's identity for offsets and consumer-side filtering.</summary>
    protected string GroupId { get; }

    private readonly IConsumer _consumer;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly BusHealth _health;

    private readonly string _topic;

    private Task? _loop;
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its inbound gate, the scope factory, the logger, the reachability tracker and its contract.</summary>
    /// <param name="consumer">The inbound gate over the Kafka client — its settings and logging handlers already wired.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message — the handler and its error and fault handlers are resolved from it.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="health">The broker-reachability tracker — every consumed delivery reports the brokers up.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets and consumer-side filtering.</param>
    protected ConsumerWorker(
        IConsumer consumer,
        IServiceScopeFactory scopeFactory,
        ILogger logger,
        IHostApplicationLifetime lifetime,
        BusHealth health,
        string topic,
        string groupId)
    {
        _consumer = consumer;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _lifetime = lifetime;
        _health = health;
        _topic = topic;
        GroupId = groupId;
    }

    /// <summary>Builds the consumer, subscribes to the topic and launches the consumer loop.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topic);

        _cancellation = new CancellationTokenSource();
        CancellationToken token = _cancellation.Token;

        _loop = Task.Run(() => Consume(token), CancellationToken.None);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Cancels the consumer loop, awaits it and closes the consumer gracefully — <c>Close</c> commits
    /// the final stored offsets. Under static membership (<c>GroupInstanceId</c>, the default) a
    /// closing member does not leave the group: a restart within the session timeout reclaims its
    /// assignment with no rebalance, and eviction is by session timeout only. When the shutdown's
    /// grace period runs out before the loop ends, the worker is abandoned instead of failing the
    /// host's stop: disposing under a live loop is unsafe, so the consumer is evicted by session
    /// timeout and the process teardown reclaims it.
    /// </summary>
    /// <param name="cancellationToken">A token bounding how long the stop may wait.</param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cancellation is null || _loop is null) return;

        _cancellation.Cancel();

        try
        {
            await _loop.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.WorkerAbandoned)) _logger.LogWarning("Stop canceled.");

            return;
        }

        _consumer.Close();
        _consumer.Dispose();

        _cancellation.Dispose();
        _cancellation = null;
        _loop = null;
    }

    /// <summary>Builds the concrete context for a delivery from its transport.</summary>
    /// <param name="result">The delivered message.</param>
    /// <param name="transport">The delivery's transport.</param>
    /// <returns>The context handed to the handler.</returns>
    protected abstract TContext CreateContext(ConsumeResult<Ignore, byte[]> result, Transport transport);

    /// <summary>Invokes the concrete handler for a delivery.</summary>
    /// <param name="handler">The handler resolved from the delivery's service scope.</param>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task Handle(THandler handler, TContext context, CancellationToken cancellationToken);

    /// <summary>
    /// Hands the failed delivery to the message's error handler — resolved from the delivery's service
    /// scope, so it is clean — over the context already built (the message deserialized once), and
    /// reports how it left it.
    /// </summary>
    /// <param name="services">The delivery's service scope, the error handler is resolved from.</param>
    /// <param name="context">The failed delivery's context.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>How the error handler left the delivery.</returns>
    protected abstract Task<ErrorResult> HandleError(IServiceProvider services, TContext context, Exception exception, CancellationToken cancellationToken);

    /// <summary>
    /// Hands the broken (or relayed) delivery to the message's fault handler — resolved from the
    /// delivery's service scope, so it is clean — over the raw materials of the broken delivery (the
    /// concrete side builds its own fault context, since the body is raw either way), and reports how
    /// it left it.
    /// </summary>
    /// <param name="services">The delivery's service scope, the fault handler is resolved from.</param>
    /// <param name="body">The delivered raw body — never deserialized, it is the thing that could not be trusted.</param>
    /// <param name="transport">The broken delivery's transport.</param>
    /// <param name="exception">The failure that broke the delivery.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>How the fault handler left the delivery.</returns>
    protected abstract Task<FaultResult> HandleFault(IServiceProvider services, byte[] body, Transport transport, Exception exception, CancellationToken cancellationToken);

    /// <summary>
    /// Consumer-side filtering — whether the delivery targets other consumers and is skipped (and
    /// acked). Point-to-point consumers keep the default (never); pub/sub consumers override it.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected virtual bool Filtered(ConsumeResult<Ignore, byte[]> result) => false;

    /// <summary>
    /// The consumer loop — the delivery flow with each failure in its own lane: our shutdown exits
    /// through the while condition; consume errors back off (the client reconnects on its own; a fatal
    /// one stops the application); a malformed delivery goes to the fault handler; every other handling
    /// failure goes to the error handler, and to the fault handler when it reports
    /// <see cref="ErrorResult.Faulted"/> or fails itself (<see cref="ErrorResult.Unhandled"/>). A
    /// dealt-with delivery is acked; one that not even the fault handler could park is logged
    /// critical with its coordinates — the recovery signal.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IDisposable scope = BusLogger.WorkerContext(_topic, GroupId);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<Ignore, byte[]>? result = null;
            IDisposable? logContext = null;

            try
            {
                result = _consumer.Consume(cancellationToken);

                // a delivery in hand proves the brokers are reachable — report it before handling,
                // whose failures are the delivery's problem, not the connection's.
                _health.Up();

                logContext = BusLogger.ConsumerContext(result);

                if (Filtered(result))
                {
                    Store(result);

                    continue;
                }

                await Deliver(result, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                using (BusLogger.DescriptionContext(BusLoggerDescriptions.WorkerStopped)) _logger.LogInformation("Consume canceled.");
            }
            catch (ConsumeException exception) when (exception.Error.IsFatal)
            {
                using (BusLogger.DescriptionContext(BusLoggerDescriptions.ApplicationStopped)) _logger.LogCritical(exception, "Consume failed.");

                _lifetime.StopApplication();

                return;
            }
            catch (Exception exception)
            {
                using (BusLogger.DescriptionContext(BusLoggerDescriptions.ConsumeRetried)) _logger.LogError(exception, "Consume failed.");

                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
            finally
            {
                logContext?.Dispose();
            }
        }
    }

    /// <summary>
    /// The per-delivery flow inside its own service scope: rebuild the context, handle it (the handler
    /// resolved from the scope) and store the offset. The two steps fail into different lanes — a
    /// context that cannot be built is a malformed delivery and goes to the fault handler, while any
    /// exception thrown by the handler itself (whatever its type) goes to the error handler and its
    /// retry ladder (which still relays to the fault handler when it reports
    /// <see cref="ErrorResult.Faulted"/>, e.g. an envelope whose lazy getters cannot be read). The
    /// handler and its error and fault handlers are all resolved from this scope and disposed —
    /// asynchronously — when the delivery ends, so each failure gets a clean handler; a disposal that
    /// throws surfaces to the loop rather than tearing it down. A shutdown cancellation is rethrown for
    /// the loop to exit through.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <param name="cancellationToken">The loop's cancellation token.</param>
    private async Task Deliver(ConsumeResult<Ignore, byte[]> result, CancellationToken cancellationToken)
    {
        await using AsyncServiceScope services = _scopeFactory.CreateAsyncScope();

        TContext context;

        try
        {
            context = CreateContext(result, Transport.Create(result));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            await Fault(services.ServiceProvider, result, exception, cancellationToken);

            return;
        }

        try
        {
            await Handle(services.ServiceProvider.GetRequiredService<THandler>(), context, cancellationToken);

            Store(result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            await Error(services.ServiceProvider, result, context, exception, cancellationToken);
        }
    }

    /// <summary>
    /// A handling failure: hands the delivery to the error handler resolved from its scope and acts on
    /// its verdict — a requeue, schedule or park acks it; a report of
    /// <see cref="ErrorResult.Faulted"/> (or a context that never built) hands it to the fault
    /// handler. An error handler that FAILS — its produce reports
    /// <see cref="ErrorResult.Unhandled"/> or it throws — also escalates to the fault handler: an
    /// unmanageable failure belongs to the fault lane, where the message stays available even when
    /// the retry lane's infrastructure is broken.
    /// </summary>
    private async Task Error(IServiceProvider services, ConsumeResult<Ignore, byte[]> result, TContext? context, Exception exception, CancellationToken cancellationToken)
    {
        ErrorResult outcome;

        try
        {
            outcome = context is not null
                ? await HandleError(services, context, exception, cancellationToken)
                : ErrorResult.Faulted;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.EscalatedToFaultHandler)) _logger.LogError(failure, "Error handler failed.");

            outcome = ErrorResult.Unhandled;
        }

        switch (outcome)
        {
            case ErrorResult.Retried or ErrorResult.Scheduled or ErrorResult.Parked:
                Store(result);
                break;

            case ErrorResult.Faulted or ErrorResult.Unhandled:
                await Fault(services, result, exception, cancellationToken);
                break;
        }
    }

    /// <summary>
    /// A broken (or relayed) delivery: hands it to the fault handler resolved from its scope and acks
    /// it when the handler parks it; an unparked delivery is logged as an error — its coordinates
    /// already travel in the delivery scope (not acked: the next commit on its partition buries it,
    /// so the log is the recovery signal — restart before that for a redelivery, or re-inject from
    /// the topic). Never throws for a failure — a fault handler that itself fails leaves the delivery
    /// unacked rather than tearing down the loop; a shutdown cancellation is rethrown, unlogged, for
    /// the loop to exit through.
    /// </summary>
    private async Task Fault(IServiceProvider services, ConsumeResult<Ignore, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        try
        {
            FaultResult outcome = await HandleFault(services, result.Message.Value, Transport.Create(result), exception, cancellationToken);

            if (outcome is FaultResult.Parked)
            {
                Store(result);

                return;
            }

            // a shutdown cancellation is not a failure: the park was reported undone because the
            // produce was canceled — the delivery stays unacked and a restart redelivers it.
            if (cancellationToken.IsCancellationRequested) return;

            // both lanes are down: the delivery could not be parked anywhere. Not acked — but a later
            // delivery on this partition will commit past it, so this alert is the recovery signal:
            // restart before that happens (redelivery) or re-inject from the topic while it is retained.
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryBuried)) _logger.LogError(exception, "Fault park failed.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // shutting down, not a failure — rethrown for the loop to exit through.
            throw;
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryBuried)) _logger.LogError(failure, "Fault handler failed.");
        }
    }

    private void Store(ConsumeResult<Ignore, byte[]> result)
    {
        try
        {
            _consumer.StoreOffset(result);
        }
        catch (KafkaException exception) when (exception.Error.Code == ErrorCode.Local_State)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RedeliveredToNewOwner)) _logger.LogWarning("Partition lost.");
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogWarning(exception, "Store failed.");
        }
    }
}
