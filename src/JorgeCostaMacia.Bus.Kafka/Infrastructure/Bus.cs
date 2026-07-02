using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka bus. Sends commands and publishes events through a shared
/// <see cref="IProducer{TKey, TValue}"/> using <c>ProduceAsync</c> (a completed task means the broker
/// acked; a failure throws). Send/Publish orchestrate: they resolve the topic, prepare the envelope
/// (fresh, or continued from an inbound transport) and produce. The consume side lives in the
/// <see cref="Worker"/>, hosted in the application lifecycle.
/// </summary>
public sealed class Bus : IBus
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly IReadOnlyDictionary<Type, IMessageConfiguration> _messages;

    /// <summary>Creates the bus over a shared producer and the message topic configurations.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="messages">The per-message topic configurations (command and event).</param>
    public Bus(IProducer<Null, byte[]> producer, IEnumerable<IMessageConfiguration> messages)
    {
        _producer = producer;
        _messages = messages.ToDictionary(configuration => configuration.MessageType);
    }

    /// <inheritdoc />
    public Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : ICommand
    {
        string topic = Topic(message);

        return Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public Task Send<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : ICommand
    {
        string topic = Topic(message);

        return Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    /// <inheritdoc />
    public Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : IEvent
    {
        string topic = Topic(message);

        return Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public Task Publish<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : IEvent
    {
        string topic = Topic(message);

        return Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    private string Topic<TMessage>(TMessage message)
    {
        Type type = message!.GetType();

        if (!_messages.TryGetValue(type, out IMessageConfiguration? configuration))
        {
            throw new InvalidOperationException($"No topic is configured for message type '{type.FullName}'.");
        }

        return configuration.TopicSpecification.Name;
    }

    /// <summary>
    /// Builds the message with a fresh envelope: a new message id, the conversation begins here (its
    /// id/address/time mirror this message), no origin, counters at zero. The domain trace comes from
    /// the message itself.
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
            { TransportHeaders.AggregateDestinationAddresses, Bytes(message.AggregateDestinationAddresses) },
            { TransportHeaders.RetryCount, Bytes("0") },
            { TransportHeaders.RedeliveryCount, Bytes("0") }
        };

        return new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(message, type), Headers = headers };
    }

    /// <summary>
    /// Builds the message continuing an inbound flow: the inbound envelope is cloned from the
    /// <paramref name="transport"/>, the message-level fields are re-stamped for this hop (new id/type/
    /// urn/occurred-at, origin = the inbound destination, destination = this message's topic, domain
    /// trace from the message), and the conversation and resilience counters are carried over unchanged.
    /// </summary>
    private Message<Null, byte[]> Prepare<TMessage>(string topic, TMessage message, ITransport transport)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Guid messageId = GuidFactory.Domain.GuidFactory.Create();
        Type type = message.GetType();
        Transport inbound = (Transport)transport;

        Headers headers = inbound.CloneHeaders();

        Restamp(headers, TransportHeaders.MessageId, Bytes(messageId));
        Restamp(headers, TransportHeaders.MessageType, Bytes(type.FullName ?? type.Name));
        Restamp(headers, TransportHeaders.MessageTypeUrn, Bytes(UrnFactory.Domain.UrnFactory.Create(type)));
        Restamp(headers, TransportHeaders.MessageOriginAddress, Bytes(inbound.GetString(TransportHeaders.MessageDestinationAddress)));
        Restamp(headers, TransportHeaders.MessageDestinationAddress, Bytes(topic));
        Restamp(headers, TransportHeaders.MessageOccurredAt, Bytes(DateTime.UtcNow.ToString("O")));
        Restamp(headers, TransportHeaders.AggregateId, Bytes(message.AggregateId));
        Restamp(headers, TransportHeaders.AggregateCorrelationId, Bytes(message.AggregateCorrelationId));
        Restamp(headers, TransportHeaders.AggregateOccurredAt, Bytes(message.AggregateOccurredAt.ToString("O")));
        Restamp(headers, TransportHeaders.AggregateDestinationAddresses, Bytes(message.AggregateDestinationAddresses));

        return new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(message, type), Headers = headers };
    }

    private Task<DeliveryResult<Null, byte[]>> Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken)
    {
        return _producer.ProduceAsync(topic, message, cancellationToken);
    }

    private static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Bytes(Guid value) => value.ToByteArray();

    private static byte[] Bytes(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));
}
