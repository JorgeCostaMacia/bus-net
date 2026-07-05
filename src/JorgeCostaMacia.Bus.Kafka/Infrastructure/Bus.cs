using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka bus — the domain-facing facade over the outbound gate: it owns the routing map and, for
/// every <c>Send</c>/<c>Publish</c>, resolves the topic, prepares the envelope (fresh, or continued
/// from an inbound transport) and produces through the <see cref="IProducer"/> gate (a completed task
/// means the broker acked; a failure throws). It does not touch the Kafka client directly — the gate
/// does, and it is the single point every outbound byte goes through, the consumers' retries and
/// error/fault parking included. The consume side lives in the per-handler consumers
/// (<c>CommandWorker</c> / <c>EventWorker</c>).
/// </summary>
internal sealed class Bus : IBus
{
    private readonly IProducer _producer;
    private readonly IReadOnlyDictionary<Type, string> _messages;

    /// <summary>Creates the bus over the outbound gate and the type → topic routing map.</summary>
    /// <param name="producer">The outbound gate every send produces through.</param>
    /// <param name="messages">The type → topic routing map (commands and events).</param>
    public Bus(IProducer producer, IReadOnlyDictionary<Type, string> messages)
    {
        _producer = producer;
        _messages = messages;
    }

    /// <inheritdoc />
    public async Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : Command
    {
        string topic = Topic(message);

        await _producer.Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Send<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : Command
    {
        string topic = Topic(message);

        await _producer.Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : Event
    {
        string topic = Topic(message);

        await _producer.Produce(topic, Prepare(topic, message), cancellationToken);
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : Event
    {
        string topic = Topic(message);

        await _producer.Produce(topic, Prepare(topic, message, transport), cancellationToken);
    }

    /// <inheritdoc />
    public Task Send<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : Command
        => _producer.Produce(messages.Select(message => Pair(message)).ToList(), cancellationToken);

    /// <inheritdoc />
    public Task Send<T>(IEnumerable<T> messages, ITransport transport, CancellationToken cancellationToken = default)
        where T : Command
        => _producer.Produce(messages.Select(message => Pair(message, transport)).ToList(), cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : Event
        => _producer.Produce(messages.Select(message => Pair(message)).ToList(), cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(IEnumerable<T> messages, ITransport transport, CancellationToken cancellationToken = default)
        where T : Event
        => _producer.Produce(messages.Select(message => Pair(message, transport)).ToList(), cancellationToken);

    private string Topic<TMessage>(TMessage message)
    {
        Type type = message!.GetType();

        if (!_messages.TryGetValue(type, out string? topic))
        {
            throw new InvalidOperationException($"'{type.FullName}' is not mapped to a topic; map it with AddCommand/AddEvent first.");
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

    /// <summary>The (topic, message) pair for a message with a fresh envelope — the batch counterpart of a single Send/Publish.</summary>
    private KeyValuePair<string, Message<Null, byte[]>> Pair<TMessage>(TMessage message)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        string topic = Topic(message);

        return new(topic, Prepare(topic, message));
    }

    /// <summary>The (topic, message) pair for a message continuing an inbound flow — the batch counterpart of a single Send/Publish with a transport.</summary>
    private KeyValuePair<string, Message<Null, byte[]>> Pair<TMessage>(TMessage message, ITransport transport)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        string topic = Topic(message);

        return new(topic, Prepare(topic, message, transport));
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Bytes(Guid value) => value.ToByteArray();

    private static byte[] Bytes(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));
}
