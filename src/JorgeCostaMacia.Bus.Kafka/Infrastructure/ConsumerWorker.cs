using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Base for the hosted consumers — one per handler, one consumer loop on its topic (scale out by
/// running more app instances; the consumer group balances the partitions). The loop is strictly
/// sequential: a handler outrunning <c>MaxPollIntervalMs</c> (5 minutes by default) evicts the
/// consumer from the group until the next consume rejoins — raise that knob for slower handlers.
/// Its Kafka configuration
/// arrives as a ready-made builder (settings and logging handlers wired where it is composed); its
/// custom policy — topic, group id, resilience — through the constructor. The whole delivery flow and
/// its error policy live here once: consume → filter → rebuild transport/context → handle in its own
/// service scope (the whole delivery in the logging scope) → store the offset (the store is the ack;
/// the background thread commits it without blocking). Failures are handed to the
/// <see cref="ConsumerErrorHandler"/> policy — retry ladder, retry scheduler, error topic; the concrete
/// consumers plug in only what differs — building their context, invoking their handler, and the
/// pub/sub-only behaviors (consumer-side filtering and retry targeting). On shutdown the loop is
/// cancelled and the consumer closed gracefully.
/// </summary>
/// <typeparam name="TContext">The context type delivered to the handler.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where THandler : IHandler
{
    /// <summary>The consumer group id — the consumer's identity for offsets and consumer-side filtering.</summary>
    protected string GroupId { get; }

    private readonly ConsumerBuilder<Ignore, byte[]> _builder;
    private IConsumer<Ignore, byte[]>? _consumer;

    private readonly ConsumerErrorHandler _errorHandler;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly IHostApplicationLifetime _lifetime;

    private readonly string _topic;

    private Task? _loop;
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its ready-made Kafka builder, its failure policy, the scope factory, the logger and its contract.</summary>
    /// <param name="builder">The consumer builder, with the Kafka settings and logging handlers already wired.</param>
    /// <param name="errorHandler">The failure policy deciding a failed delivery's outcome — retry ladder, retry scheduler, error topic.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets and consumer-side filtering.</param>
    protected ConsumerWorker(
        ConsumerBuilder<Ignore, byte[]> builder,
        ConsumerErrorHandler errorHandler,
        IServiceScopeFactory scopeFactory,
        ILogger logger,
        IHostApplicationLifetime lifetime,
        string topic,
        string groupId)
    {
        _builder = builder;
        _errorHandler = errorHandler;
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
        _consumer = _builder.Build();
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
        if (_cancellation is null || _loop is null || _consumer is null) return;

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
        _consumer = null;

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
    /// Consumer-side filtering — whether the delivery targets other consumers and is skipped (and
    /// acked). Point-to-point consumers keep the default (never); pub/sub consumers override it.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected virtual bool Filtered(ConsumeResult<Ignore, byte[]> result) => false;

    /// <summary>
    /// Stamps the retry's targeting. Point-to-point consumers keep the default (none — the
    /// envelope travels unchanged); pub/sub consumers override it to target their own group.
    /// </summary>
    /// <param name="headers">The retry's headers.</param>
    protected virtual void Target(Headers headers) { }

    /// <summary>
    /// The consumer loop — the delivery flow with each failure in its own lane: our shutdown exits
    /// through the while condition; consume errors back off (the client reconnects on its own);
    /// malformed deliveries park to the fault topic; every other handling failure is decided by the
    /// <see cref="ConsumerErrorHandler"/> policy — retry ladder, retry scheduler, error topic. A dealt-with
    /// delivery is acked; an unresolved one is logged with the delivery attached and left unacked.
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
                result = _consumer!.Consume(cancellationToken);

                logContext = BusLogger.ConsumerContext(_logger, result);

                if (Filtered(result))
                {
                    Store(result);

                    continue;
                }

                Transport transport = Transport.Create(result);
                TContext context = CreateContext(result, transport);

                using (IServiceScope services = _scopeFactory.CreateScope())
                {
                    await Handle(services.ServiceProvider.GetRequiredService<THandler>(), context, cancellationToken);
                }

                Store(result);
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
            catch (ConsumeException exception)
            {
                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ConsumeRetried)) _logger.LogError(exception, "Consume failed.");

                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
            catch (JsonException exception)
            {
                if (await _errorHandler.Malformed(result!, exception, cancellationToken)) Store(result!);
            }
            catch (KeyNotFoundException exception)
            {
                if (await _errorHandler.Malformed(result!, exception, cancellationToken)) Store(result!);
            }
            catch (InvalidCastException exception)
            {
                if (await _errorHandler.Malformed(result!, exception, cancellationToken)) Store(result!);
            }
            catch (Exception exception) when (result is not null)
            {
                if (await _errorHandler.Handle(result, exception, Target, cancellationToken)) Store(result);
            }
            catch (Exception exception)
            {
                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ConsumeRetried)) _logger.LogError(exception, "Consume failed.");
            }
            finally
            {
                logContext?.Dispose();
            }
        }
    }

    private void Store(ConsumeResult<Ignore, byte[]> result)
    {
        try
        {
            _consumer!.StoreOffset(result);
        }
        catch (KafkaException exception) when (exception.Error.Code == ErrorCode.Local_State)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.RedeliveredToNewOwner)) _logger.LogWarning("Partition lost.");
        }
        catch (KafkaException exception)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogWarning(exception, "Store failed.");
        }
    }

    /// <summary>Replaces every value of a header key with the given one.</summary>
    /// <param name="headers">The headers to restamp.</param>
    /// <param name="key">The header key.</param>
    /// <param name="value">The new value.</param>
    protected static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}
