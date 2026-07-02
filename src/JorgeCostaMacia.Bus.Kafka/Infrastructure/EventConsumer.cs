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
    /// The consumer loop — the whole delivery flow and its error policy in one place: consume →
    /// filter (skip and ack deliveries targeting other consumers, straight off the raw header) →
    /// rebuild transport/context → handle in its own service scope (envelope trace in the logging
    /// scope) → store the offset (the store is the ack; the background thread commits it without
    /// blocking). Each failure has its own lane: our shutdown exits through the while condition;
    /// consume errors back off (the client reconnects on its own); a retryable failure requeues to
    /// the topic targeted to this group (the requeue is the ack); everything else — produce failures,
    /// malformed deliveries, handling errors — is logged with the delivery attached and left unacked,
    /// until the redelivery / error-topic policy lands.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IDisposable? scope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["Topic"] = _configuration.Topic,
            ["GroupId"] = _configuration.GroupId
        });

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<Null, byte[]>? result = null;
            IDisposable? delivery = null;

            try
            {
                result = _consumer!.Consume(cancellationToken);

                delivery = Delivery(result);

                if (Filtered(result))
                {
                    Store(result);

                    continue;
                }

                Transport transport = CreateTransport(result);
                EventContext<TEvent> context = CreateContext(result, transport);

                using (IServiceScope services = _scopeFactory.CreateScope())
                {
                    await services.ServiceProvider
                        .GetRequiredService<TEventSubscriber>()
                        .Handle(context, cancellationToken);
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
                delivery?.Dispose();
            }
        }
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

    /// <summary>
    /// Whether a failed delivery is requeued: the envelope's <c>RetryCount</c> has attempts left and
    /// the exception type is not excluded. Malformed deliveries never reach this lane — retrying the
    /// same bytes is pointless.
    /// </summary>
    private bool Retryable(ConsumeResult<Null, byte[]>? result, Exception exception)
    {
        if (result is null || !result.Message.Headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] header)) return false;

        return int.TryParse(Encoding.UTF8.GetString(header), out int retries)
            && retries < _configuration.RetryAttempts
            && !_configuration.RetryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(exception));
    }

    /// <summary>
    /// Retries through the topic itself: the delivery is requeued at the tail with the envelope
    /// cloned, <c>RetryCount</c> incremented and targeted to this group only — the other subscriber
    /// groups already handled the original, so they filter the retry out; the user's original
    /// targeting stays in the message body. Returns whether the requeue succeeded (the caller then
    /// acks the original); a failed requeue is logged and leaves the delivery unacked — nothing
    /// thrown here can escape the loop.
    /// </summary>
    private async Task<bool> Retry(ConsumeResult<Null, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        Transport transport = CreateTransport(result);
        int retry = transport.GetInt(TransportHeaders.RetryCount) + 1;

        Headers headers = transport.CloneHeaders();

        Restamp(headers, TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retry.ToString()));
        Restamp(headers, TransportHeaders.AggregateConsumers, Encoding.UTF8.GetBytes(_configuration.GroupId));

        try
        {
            await _producer.ProduceAsync(_configuration.Topic, new Message<Null, byte[]> { Value = result.Message.Value, Headers = headers }, cancellationToken);

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
    /// finally, so every log of the iteration (the subscriber's own included, and the failure lanes)
    /// is fully traced and a failed message can be inspected and reprocessed from the log platform.
    /// </summary>
    private IDisposable? Delivery(ConsumeResult<Null, byte[]> result)
    {
        Dictionary<string, object?> delivery = new()
        {
            ["Partition"] = result.Partition.Value,
            ["Offset"] = result.Offset.Value,
            ["Body"] = result.Message.Value is null ? null : Encoding.UTF8.GetString(result.Message.Value)
        };

        foreach (IHeader header in result.Message.Headers)
        {
            byte[] value = header.GetValueBytes();

            delivery[header.Key] = GuidHeaders.Contains(header.Key) && value.Length == 16
                ? new Guid(value)
                : Encoding.UTF8.GetString(value);
        }

        return _logger.BeginScope(delivery);
    }

    private static readonly ImmutableList<string> GuidHeaders =
    [
        TransportHeaders.MessageId,
        TransportHeaders.ConversationId,
        TransportHeaders.AggregateId,
        TransportHeaders.AggregateCorrelationId
    ];

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
