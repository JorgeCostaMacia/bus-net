using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one event subscriber in the application lifecycle: one consumer loop on the
/// subscriber's topic (scale out by running more app instances — the consumer group balances the
/// partitions). Each delivery rebuilds the context from the transport's headers, is handled in its
/// own service scope and acked by storing the offset after handling (committed in the background —
/// the store-offsets pattern). A failed delivery is retried through the topic itself: requeued at
/// the tail with <c>RetryCount</c> incremented and targeted to this group only. On shutdown the loop
/// is cancelled and the consumer closed gracefully.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventConsumer<TEvent, TEventSubscriber> : IHostedService
    where TEvent : Domain.Event
    where TEventSubscriber : IEventSubscriber<TEvent, EventContext<TEvent>, Transport>
{
    private readonly IHandlerConfiguration _configuration;
    private readonly IProducer<Null, byte[]> _producer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<EventConsumer<TEvent, TEventSubscriber>> _logger;
    private IConsumer<Null, byte[]>? _consumer;
    private Task? _loop;
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its subscriber configuration, the shared producer, the scope factory and the logger.</summary>
    /// <param name="configuration">The subscriber's consumer configuration.</param>
    /// <param name="producer">The shared Kafka producer, used to requeue failed deliveries.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for consumer errors, internal Kafka logs and retries.</param>
    public EventConsumer(IHandlerConfiguration configuration, IProducer<Null, byte[]> producer, IServiceScopeFactory scopeFactory, ILogger<EventConsumer<TEvent, TEventSubscriber>> logger)
    {
        _configuration = configuration;
        _producer = producer;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    /// <summary>Builds the consumer, subscribes to the topic and launches the consumer loop.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer = new ConsumerBuilder<Null, byte[]>(_configuration.ConsumerConfig)
            .SetErrorHandler((_, error) => LogError(error))
            .SetLogHandler((_, log) => Log(log))
            .Build();

        _consumer.Subscribe(_configuration.Topic);

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

    /// <summary>
    /// The consumer loop: consume → filter → handle → store the offset (the store is the ack; the
    /// background thread commits it without blocking the loop). The loop only stops on cancellation:
    /// a consume error is logged and retried (the client reconnects on its own); a failed delivery is
    /// requeued to the topic when retryable — the requeue is the ack — and otherwise logged and not
    /// acked, until the resilience policy (redelivery / error topic) lands in the next phase.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IDisposable? scope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["Topic"] = _configuration.Topic,
            ["GroupId"] = _configuration.GroupId
        });

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<Null, byte[]> result = _consumer!.Consume(cancellationToken);

                    if (Filtered(result))
                    {
                        Store(result);

                        continue;
                    }

                    await Handle(result, cancellationToken);

                    Store(result);
                }
                catch (ConsumeException exception)
                {
                    _logger.LogError(exception, "Consume failed.");

                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    _logger.LogError(exception, "Handling failed; the delivery is not acked.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful stop.
        }
    }

    /// <summary>
    /// Handles one delivery: rebuilds the context from the transport's headers and invokes the
    /// subscriber in its own service scope, with the envelope trace in the logging scope. A retryable
    /// failure is requeued here (the return then acks the original); a non-retryable one propagates
    /// (no ack).
    /// </summary>
    private async Task Handle(ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
    {
        Transport transport = CreateTransport(result);
        EventContext<TEvent> context = CreateContext(result, transport);

        using IDisposable? logging = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = context.MessageId,
            ["ConversationId"] = context.ConversationId,
            ["AggregateId"] = context.AggregateId,
            ["AggregateCorrelationId"] = context.AggregateCorrelationId
        });

        try
        {
            using IServiceScope scope = _scopeFactory.CreateScope();

            TEventSubscriber subscriber = scope.ServiceProvider.GetRequiredService<TEventSubscriber>();

            await subscriber.Handle(context, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException
            && context.RetryCount < _configuration.RetryAttempts
            && !_configuration.RetryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(exception)))
        {
            await Retry(transport, result, exception, cancellationToken);
        }
    }

    /// <summary>
    /// Retries through the topic itself: the delivery is requeued at the tail with the envelope
    /// cloned, <c>RetryCount</c> incremented and targeted to this group only — the other subscriber
    /// groups already handled the original, so they filter the retry out. The user's original
    /// targeting stays in the message body; the header is this copy's delivery instruction. Nothing
    /// is held in memory and the retry survives a restart.
    /// </summary>
    private async Task Retry(Transport transport, ConsumeResult<Null, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        int retry = transport.GetInt(TransportHeaders.RetryCount) + 1;

        Headers headers = transport.CloneHeaders();

        Restamp(headers, TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retry.ToString()));
        Restamp(headers, TransportHeaders.AggregateConsumers, Encoding.UTF8.GetBytes(_configuration.GroupId));

        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["Retry"] = retry
        }))
        {
            _logger.LogWarning(exception, "Handling failed; requeued to retry.");
        }

        await _producer.ProduceAsync(_configuration.Topic, new Message<Null, byte[]> { Value = result.Message.Value, Headers = headers }, cancellationToken);
    }

    /// <summary>
    /// Consumer-side filtering: when the message targets specific consumers
    /// (<c>AggregateConsumers</c> header non-empty) and this group is not among them, the delivery is
    /// skipped (and acked) without deserializing the body.
    /// </summary>
    private bool Filtered(ConsumeResult<Null, byte[]> result)
    {
        if (!result.Message.Headers.TryGetLastBytes(TransportHeaders.AggregateConsumers, out byte[] header)) return false;

        string consumers = Encoding.UTF8.GetString(header);

        if (string.IsNullOrWhiteSpace(consumers)) return false;

        return !consumers
            .Split(',')
            .Select(consumer => consumer.Trim())
            .Contains(_configuration.GroupId);
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

    private static EventContext<TEvent> CreateContext(ConsumeResult<Null, byte[]> result, Transport transport)
        => new(
            JsonSerializer.Deserialize<TEvent>(result.Message.Value)!,
            transport,
            transport.GetGuid(TransportHeaders.MessageId),
            transport.GetString(TransportHeaders.MessageType),
            transport.GetStringList(TransportHeaders.MessageTypeUrn),
            transport.GetString(TransportHeaders.MessageDestinationAddress),
            transport.GetStringOrDefault(TransportHeaders.MessageOriginAddress),
            transport.GetDateTime(TransportHeaders.MessageOccurredAt),
            transport.GetGuid(TransportHeaders.ConversationId),
            transport.GetString(TransportHeaders.ConversationAddress),
            transport.GetDateTime(TransportHeaders.ConversationOccurredAt),
            transport.GetStringList(TransportHeaders.AggregateConsumers),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

    private static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}
