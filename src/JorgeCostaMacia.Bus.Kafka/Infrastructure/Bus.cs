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
/// acked; a failure throws). Each message's topic is resolved from the registered
/// <see cref="IMessageConfiguration"/> set. Consuming (Start/Stop) and continuing from an inbound
/// transport (propagation) are added in later phases.
/// </summary>
public sealed class Bus : IBus
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly IReadOnlyDictionary<Type, IMessageConfiguration> _messages;
    private readonly ISerializer _serializer;

    /// <summary>Creates the bus over a shared producer, the message topic configurations and a serializer.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="messages">The per-message topic configurations (command and event).</param>
    /// <param name="serializer">The message body serializer.</param>
    public Bus(IProducer<Null, byte[]> producer, IEnumerable<IMessageConfiguration> messages, ISerializer serializer)
    {
        _producer = producer;
        _messages = messages.ToDictionary(configuration => configuration.MessageType);
        _serializer = serializer;
    }

    /// <inheritdoc />
    public Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : ICommand
        => Produce(message, cancellationToken);

    /// <inheritdoc />
    public Task Send<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : ICommand
        => throw new NotImplementedException("Continuing from an inbound transport is built in a later phase.");

    /// <inheritdoc />
    public Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : IEvent
        => Produce(message, cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : IEvent
        => throw new NotImplementedException("Continuing from an inbound transport is built in a later phase.");

    /// <inheritdoc />
    public Task Start(CancellationToken cancellationToken = default)
        => throw new NotImplementedException("The consumer is built in a later phase.");

    /// <inheritdoc />
    public Task Stop(CancellationToken cancellationToken = default)
        => throw new NotImplementedException("The consumer is built in a later phase.");

    private async Task Produce<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Type type = message.GetType();

        if (!_messages.TryGetValue(type, out IMessageConfiguration? configuration))
        {
            throw new InvalidOperationException($"No topic is configured for message type '{type.FullName}'.");
        }

        string topic = configuration.TopicSpecification.Name;

        Message<Null, byte[]> kafkaMessage = new()
        {
            Value = _serializer.Serialize(message),
            Headers = HeadersFactory.CreateNew(topic, message)
        };

        await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);
    }
}
