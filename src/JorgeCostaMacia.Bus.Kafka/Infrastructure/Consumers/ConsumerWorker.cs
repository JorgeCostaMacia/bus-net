using System.Text.Json;
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

    private readonly string _topic;

    private Task? _loop;
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its inbound gate, the scope factory, the logger and its contract.</summary>
    /// <param name="consumer">The inbound gate over the Kafka client — its settings and logging handlers already wired.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message — the handler and its error and fault handlers are resolved from it.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets and consumer-side filtering.</param>
    protected ConsumerWorker(
        IConsumer consumer,
        IServiceScopeFactory scopeFactory,
        ILogger logger,
        IHostApplicationLifetime lifetime,
        string topic,
        string groupId)
    {
        _consumer = consumer;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _lifetime = lifetime;
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
    /// Cancels the consumer loop, awaits it and closes the consumer gracefully — final offsets
    /// committed. With the default static membership (<c>GroupInstanceId</c>) closing does NOT leave
    /// the group: the assignment is retained for a restart within the session timeout, so rolling
    /// deploys do not rebalance. When the shutdown's grace period runs out before the loop ends, the
    /// worker is abandoned instead of failing the host's stop: disposing under a live loop is
    /// unsafe, so the consumer is evicted by session timeout and the process teardown reclaims it.
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
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.WorkerAbandoned)) _logger.LogWarning("Stop canceled.");

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
    /// failure goes to the error handler, and to the fault handler as the relay when it reports
    /// <see cref="ErrorResult.Faulted"/>. A dealt-with delivery is acked; an unresolved one is
    /// left unacked and redelivers.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IDisposable? scope = BusLogger.WorkerContext(_logger, _topic, GroupId);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<Ignore, byte[]>? result = null;
            IDisposable? logContext = null;

            try
            {
                result = _consumer.Consume(cancellationToken);

                logContext = BusLogger.ConsumerContext(_logger, result);

                if (Filtered(result))
                {
                    Store(result);

                    continue;
                }

                await Deliver(result, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.WorkerStopped)) _logger.LogInformation("Consume canceled.");
            }
            catch (ConsumeException exception) when (exception.Error.IsFatal)
            {
                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ApplicationStopped)) _logger.LogCritical(exception, "Consume failed.");

                _lifetime.StopApplication();

                return;
            }
            catch (Exception exception)
            {
                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ConsumeRetried)) _logger.LogError(exception, "Consume failed.");

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
    /// resolved from the scope) and store the offset; a malformed delivery goes to the fault handler,
    /// every other handling failure to the error handler (which relays to the fault handler when it
    /// reports <see cref="ErrorResult.Faulted"/>). The handler and its error and fault handlers are all
    /// resolved from this scope and disposed — asynchronously — when the delivery ends, so each failure
    /// gets a clean handler; a disposal that throws surfaces to the loop rather than tearing it down. A
    /// shutdown cancellation is rethrown for the loop to exit through.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <param name="cancellationToken">The loop's cancellation token.</param>
    private async Task Deliver(ConsumeResult<Ignore, byte[]> result, CancellationToken cancellationToken)
    {
        await using AsyncServiceScope services = _scopeFactory.CreateAsyncScope();

        TContext? context = default;

        try
        {
            context = CreateContext(result, Transport.Create(result));

            await Handle(services.ServiceProvider.GetRequiredService<THandler>(), context, cancellationToken);

            Store(result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (JsonException exception)
        {
            await Fault(services.ServiceProvider, result, exception, cancellationToken);
        }
        catch (KeyNotFoundException exception)
        {
            await Fault(services.ServiceProvider, result, exception, cancellationToken);
        }
        catch (InvalidCastException exception)
        {
            await Fault(services.ServiceProvider, result, exception, cancellationToken);
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
    /// handler; anything else leaves it unacked to redeliver.
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
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogCritical(failure, "Error handler failed.");

            return;
        }

        switch (outcome)
        {
            case ErrorResult.Retried or ErrorResult.Scheduled or ErrorResult.Parked:
                Store(result);
                break;

            case ErrorResult.Faulted:
                await Fault(services, result, exception, cancellationToken);
                break;
        }
    }

    /// <summary>
    /// A broken (or relayed) delivery: hands it to the fault handler resolved from its scope and acks
    /// it when the handler parks it; an unparked delivery is left unacked to redeliver. Never throws —
    /// a fault handler that itself fails leaves the delivery unacked rather than tearing down the loop.
    /// </summary>
    private async Task Fault(IServiceProvider services, ConsumeResult<Ignore, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        try
        {
            FaultResult outcome = await HandleFault(services, result.Message.Value, Transport.Create(result), exception, cancellationToken);

            if (outcome is FaultResult.Parked) Store(result);
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogCritical(failure, "Fault handler failed.");
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
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.RedeliveredToNewOwner)) _logger.LogWarning("Partition lost.");
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogWarning(exception, "Store failed.");
        }
    }
}
