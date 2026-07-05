using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka bus — the owner of the routing map and the single outbound gate: every send in the
/// application goes through it, the consumers' machinery included (retries and error parking use its
/// internal produce). Sends commands and publishes events with <c>ProduceAsync</c> (a completed task
/// means the broker acked; a failure throws). Send/Publish orchestrate: they resolve the topic,
/// prepare the envelope (fresh, or continued from an inbound transport) and produce. It only uses the
/// shared producer — the lifecycle (flush on shutdown, disposal) is the <c>ProducerWorker</c>'s and
/// the container's. The consume side lives in the per-handler consumers (<c>CommandWorker</c> /
/// <c>EventWorker</c>).
/// </summary>
public sealed class Bus : IBus
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly IReadOnlyDictionary<Type, string> _messages;
    private readonly ILogger<Bus> _logger;

    /// <summary>Creates the bus over a shared producer, the type → topic routing map and the logger.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="messages">The type → topic routing map (commands and events).</param>
    /// <param name="logger">The logger for produce failures.</param>
    public Bus(IProducer<Null, byte[]> producer, IReadOnlyDictionary<Type, string> messages, ILogger<Bus> logger)
    {
        _producer = producer;
        _messages = messages;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : Command
    {
        string topic = Topic(message);

        await Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Send<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : Command
    {
        string topic = Topic(message);

        await Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : Event
    {
        string topic = Topic(message);

        await Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : Event
    {
        string topic = Topic(message);

        await Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    private string Topic<TMessage>(TMessage message)
    {
        Type type = message!.GetType();

        if (!_messages.TryGetValue(type, out string? topic))
        {
            throw new InvalidOperationException($"No topic is configured for message type '{type.FullName}'.");
        }

        return topic;
    }

    /// <summary>
    /// Builds the message with a fresh envelope: a new message id, the conversation begins here (its
    /// id/address/time mirror this message), no origin, the retry counter at zero. The domain
    /// trace comes from the message itself.
    /// </summary>
    private Message<Null, byte[]> Prepare<TMessage>(string topic, TMessage message)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Guid messageId = GuidFactory.Domain.GuidFactory.Create();
        string occurredAt = DateTime.UtcNow.ToString("O");
        Type type = message.GetType();

        Headers headers = new()
        {
            { TransportHeaders.MessageId, Bytes(messageId) },
            { TransportHeaders.MessageType, Bytes(type.FullName ?? type.Name) },
            { TransportHeaders.MessageTypeUrn, Bytes(UrnFactory.Domain.UrnFactory.Create(type)) },
            { TransportHeaders.MessageDestinationAddress, Bytes(topic) },
            { TransportHeaders.MessageOccurredAt, Bytes(occurredAt) },
            { TransportHeaders.ConversationId, Bytes(messageId) },
            { TransportHeaders.ConversationAddress, Bytes(topic) },
            { TransportHeaders.ConversationOccurredAt, Bytes(occurredAt) },
            { TransportHeaders.AggregateId, Bytes(message.AggregateId) },
            { TransportHeaders.AggregateCorrelationId, Bytes(message.AggregateCorrelationId) },
            { TransportHeaders.AggregateOccurredAt, Bytes(message.AggregateOccurredAt.ToString("O")) },
            { TransportHeaders.AggregateConsumers, Bytes(message.AggregateConsumers) },
            { TransportHeaders.RetryCount, Bytes("0") }
        };

        return new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(message, type), Headers = headers };
    }

    /// <summary>
    /// Builds the message continuing an inbound flow: the inbound envelope is cloned from the
    /// <paramref name="transport"/>, the message-level fields are re-stamped for this hop (new id/type/
    /// urn/occurred-at, origin = the inbound destination, destination = this message's topic, domain
    /// trace from the message), and the conversation and the retry counter are carried over unchanged.
    /// </summary>
    private Message<Null, byte[]> Prepare<TMessage>(string topic, TMessage message, ITransport transport)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Guid messageId = GuidFactory.Domain.GuidFactory.Create();
        Type type = message.GetType();
        Transport inbound = (Transport)transport;

        Headers headers = inbound.CloneHeaders();

        headers.Restamp(TransportHeaders.MessageId, Bytes(messageId));
        headers.Restamp(TransportHeaders.MessageType, Bytes(type.FullName ?? type.Name));
        headers.Restamp(TransportHeaders.MessageTypeUrn, Bytes(UrnFactory.Domain.UrnFactory.Create(type)));
        headers.Restamp(TransportHeaders.MessageOriginAddress, Bytes(inbound.GetString(TransportHeaders.MessageDestinationAddress)));
        headers.Restamp(TransportHeaders.MessageDestinationAddress, Bytes(topic));
        headers.Restamp(TransportHeaders.MessageOccurredAt, Bytes(DateTime.UtcNow.ToString("O")));
        headers.Restamp(TransportHeaders.AggregateId, Bytes(message.AggregateId));
        headers.Restamp(TransportHeaders.AggregateCorrelationId, Bytes(message.AggregateCorrelationId));
        headers.Restamp(TransportHeaders.AggregateOccurredAt, Bytes(message.AggregateOccurredAt.ToString("O")));
        headers.Restamp(TransportHeaders.AggregateConsumers, Bytes(message.AggregateConsumers));

        return new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(message, type), Headers = headers };
    }

    /// <summary>
    /// Produces a message — the single gate every outbound byte goes through (Send/Publish envelopes
    /// and the consumers' retries and error parking alike). A failure is logged with the outbound
    /// delivery attached (topic, body and envelope — inspectable and reinjectable from the log
    /// platform) and rethrown: the caller's task still faults, awaiting still means broker-acked.
    /// </summary>
    internal async Task<DeliveryResult<Null, byte[]>> Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken)
    {
        try
        {
            return await _producer.ProduceAsync(topic, message, cancellationToken);
        }
        catch (ProduceException<Null, byte[]> exception) when (exception.Error.Code == ErrorCode.Local_QueueFull)
        {
            using (BusLogger.ProducerContext(_logger, topic, message))
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ProducerQueueFull))
            {
                _logger.LogError(exception, "Producer failed.");
            }

            throw;
        }
        catch (ProduceException<Null, byte[]> exception)
        {
            using (BusLogger.ProducerContext(_logger, topic, message))
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.SendFaulted))
            {
                _logger.LogError(exception, "Producer failed.");
            }

            throw;
        }
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Bytes(Guid value) => value.ToByteArray();

    private static byte[] Bytes(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));
}
