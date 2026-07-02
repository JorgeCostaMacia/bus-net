using System.Collections.Immutable;
using System.Text;
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
/// running more app instances; the consumer group balances the partitions). The whole delivery flow
/// and its error policy live here once: consume → filter → rebuild transport/context → handle in its
/// own service scope (the whole delivery in the logging scope) → store the offset (the store is the
/// ack; the background thread commits it without blocking). A retryable failure is requeued through
/// the topic itself; the concrete consumers plug in only what differs — building their context,
/// invoking their handler, and the pub/sub-only behaviors (consumer-side filtering and retry
/// targeting). On shutdown the loop is cancelled and the consumer closed gracefully.
/// </summary>
/// <typeparam name="TContext">The context type delivered to the handler.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class Consumer<TContext, THandler> : IHostedService
    where THandler : IHandler
{
    private static readonly ImmutableList<string> GuidHeaders =
    [
        TransportHeaders.MessageId,
        TransportHeaders.ConversationId,
        TransportHeaders.AggregateId,
        TransportHeaders.AggregateCorrelationId
    ];

    private readonly IProducer<Null, byte[]> _producer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private IConsumer<Null, byte[]>? _consumer;
    private Task? _loop;
    private CancellationTokenSource? _cancellation;

    /// <summary>The handler's consumer configuration (topic, group id, resilience policy, consumer settings).</summary>
    protected IHandlerConfiguration Configuration { get; }

    /// <summary>Creates the consumer over its handler configuration, the shared producer, the scope factory and the logger.</summary>
    /// <param name="configuration">The handler's consumer configuration.</param>
    /// <param name="producer">The shared Kafka producer, used to requeue failed deliveries.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for consumer errors, internal Kafka logs and retries.</param>
    protected Consumer(IHandlerConfiguration configuration, IProducer<Null, byte[]> producer, IServiceScopeFactory scopeFactory, ILogger logger)
    {
        Configuration = configuration;
        _producer = producer;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    /// <summary>Builds the consumer, subscribes to the topic and launches the consumer loop.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer = new ConsumerBuilder<Null, byte[]>(Configuration.ConsumerConfig)
            .SetErrorHandler((_, error) => LogError(error))
            .SetLogHandler((_, log) => Log(log))
            .Build();

        _consumer.Subscribe(Configuration.Topic);

        _cancellation = new CancellationTokenSource();
        CancellationToken token = _cancellation.Token;

        _loop = Task.Run(() => Consume(token), CancellationToken.None);

        return Task.CompletedTask;
    }

    /// <summary>Cancels the consumer loop, awaits it and closes the consumer gracefully.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cancellation is null || _loop is null || _consumer is null) return;

        _cancellation.Cancel();

        await _loop.WaitAsync(cancellationToken);

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
    protected abstract TContext CreateContext(ConsumeResult<Null, byte[]> result, Transport transport);

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
    protected virtual bool Filtered(ConsumeResult<Null, byte[]> result) => false;

    /// <summary>
    /// Stamps the requeued retry's targeting. Point-to-point consumers keep the default (none — the
    /// envelope travels unchanged); pub/sub consumers override it to target their own group.
    /// </summary>
    /// <param name="headers">The requeued delivery's headers.</param>
    protected virtual void Target(Headers headers) { }

    /// <summary>
    /// The consumer loop — the whole delivery flow and its error policy in one place. Each failure
    /// has its own lane: our shutdown exits through the while condition; consume errors back off (the
    /// client reconnects on its own); a retryable failure requeues to the topic (the requeue is the
    /// ack); everything else — produce failures, malformed deliveries, handling errors — is logged
    /// with the delivery attached and left unacked, until the redelivery / error-topic policy lands.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IDisposable? scope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["Topic"] = Configuration.Topic,
            ["GroupId"] = Configuration.GroupId
        });

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<Null, byte[]>? result = null;
            IDisposable? logContext = null;

            try
            {
                result = _consumer!.Consume(cancellationToken);

                logContext = LogContext(result);

                if (Filtered(result))
                {
                    Store(result);

                    continue;
                }

                Transport transport = CreateTransport(result);
                TContext context = CreateContext(result, transport);

                using (IServiceScope services = _scopeFactory.CreateScope())
                {
                    await Handle(services.ServiceProvider.GetRequiredService<THandler>(), context, cancellationToken);
                }

                Store(result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Consume canceled.");
            }
            catch (ConsumeException exception)
            {
                _logger.LogError(exception, "Consume failed.");

                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
            catch (ProduceException<Null, byte[]> exception)
            {
                _logger.LogError(exception, "Produce failed; the delivery is not acked.");
            }
            catch (JsonException exception)
            {
                _logger.LogError(exception, "Malformed delivery; the body cannot be deserialized.");
            }
            catch (KeyNotFoundException exception)
            {
                _logger.LogError(exception, "Malformed delivery; an envelope header is missing.");
            }
            catch (InvalidCastException exception)
            {
                _logger.LogError(exception, "Malformed delivery; an envelope header is invalid.");
            }
            catch (Exception exception) when (Retryable(result, exception))
            {
                if (await Retry(result!, exception, cancellationToken)) Store(result!);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Handling failed; the delivery is not acked.");
            }
            finally
            {
                logContext?.Dispose();
            }
        }
    }

    /// <summary>
    /// Whether a failed delivery is requeued: the envelope's <c>RetryCount</c> has attempts left and
    /// the exception type is not excluded. Malformed deliveries never reach this lane — retrying the
    /// same bytes is pointless.
    /// </summary>
    private bool Retryable(ConsumeResult<Null, byte[]>? result, Exception exception)
    {
        if (result is null || !result.Message.Headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] header)) return false;

        return int.TryParse(Encoding.UTF8.GetString(header), out int retries)
            && retries < Configuration.RetryAttempts
            && !Configuration.RetryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(exception));
    }

    /// <summary>
    /// Retries through the topic itself: the delivery is requeued at the tail with the envelope
    /// cloned, <c>RetryCount</c> incremented and the concrete consumer's targeting stamped — nothing
    /// is held in memory and the retry survives a restart. Returns whether the requeue succeeded (the
    /// caller then acks the original); a failed requeue is logged and leaves the delivery unacked —
    /// nothing thrown here can escape the loop.
    /// </summary>
    private async Task<bool> Retry(ConsumeResult<Null, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        Transport transport = CreateTransport(result);
        int retry = transport.GetInt(TransportHeaders.RetryCount) + 1;

        Headers headers = transport.CloneHeaders();

        Restamp(headers, TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retry.ToString()));

        Target(headers);

        try
        {
            await _producer.ProduceAsync(Configuration.Topic, new Message<Null, byte[]> { Value = result.Message.Value, Headers = headers }, cancellationToken);

            using (_logger.BeginScope(new Dictionary<string, object?>
            {
                ["Retry"] = retry
            }))
            {
                _logger.LogWarning(exception, "Handling failed; requeued to retry.");
            }

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ProduceException<Null, byte[]> produce)
        {
            _logger.LogError(produce, "Produce failed; the delivery is not acked.");

            return false;
        }
    }

    private void Store(ConsumeResult<Null, byte[]> result)
    {
        try
        {
            _consumer!.StoreOffset(result);
        }
        catch (KafkaException exception) when (exception.Error.Code == ErrorCode.Local_State)
        {
            _logger.LogWarning("Partition lost in a rebalance; its new owner will handle the message again.");
        }
    }

    /// <summary>
    /// Logging scope carrying the whole delivery — the partition/offset pointer to refetch it, the
    /// raw body and every header — opened right after consume and disposed in the iteration's
    /// finally, so every log of the iteration (the handler's own included, and the failure lanes) is
    /// fully traced and a failed message can be inspected and reprocessed from the log platform.
    /// </summary>
    private IDisposable? LogContext(ConsumeResult<Null, byte[]> result)
    {
        Dictionary<string, object?> logContext = new()
        {
            ["Partition"] = result.Partition.Value,
            ["Offset"] = result.Offset.Value,
            ["Body"] = result.Message.Value is null ? null : Encoding.UTF8.GetString(result.Message.Value)
        };

        foreach (IHeader header in result.Message.Headers)
        {
            byte[] value = header.GetValueBytes();

            logContext[header.Key] = GuidHeaders.Contains(header.Key) && value.Length == 16
                ? new Guid(value)
                : Encoding.UTF8.GetString(value);
        }

        return _logger.BeginScope(logContext);
    }

    private void LogError(Error error)
    {
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["@Error"] = error
        }))
        {
            _logger.LogError("Consumer error.");
        }
    }

    private void Log(LogMessage log)
    {
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["Name"] = log.Name,
            ["Facility"] = log.Facility
        }))
        {
            _logger.Log((LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging), "{Message}", log.Message);
        }
    }

    private static Transport CreateTransport(ConsumeResult<Null, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);

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
