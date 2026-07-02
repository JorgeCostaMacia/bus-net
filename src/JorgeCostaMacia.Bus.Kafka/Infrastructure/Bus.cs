using System.Collections.Immutable;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka bus. Sends commands and publishes events through a shared
/// <see cref="IProducer{TKey, TValue}"/> using <c>ProduceAsync</c> (a completed task means the broker
/// acked; a failure throws). Send/Publish orchestrate: they resolve the topic, prepare the envelope
/// (fresh, or continued from an inbound transport) and produce. Start launches the consumers — one
/// loop per handler configuration and concurrency slot — each delivery handled in its own service
/// scope and acked by committing the offset; Stop cancels the loops and closes the consumers.
/// </summary>
public sealed class Bus : IBus
{
    private static readonly MethodInfo HandleCommandMethod = typeof(Bus).GetMethod(nameof(HandleCommand), BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly MethodInfo HandleEventMethod = typeof(Bus).GetMethod(nameof(HandleEvent), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private readonly IProducer<Null, byte[]> _producer;
    private readonly IReadOnlyDictionary<Type, IMessageConfiguration> _messages;
    private readonly IReadOnlyList<IHandlerConfiguration> _handlers;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly List<Task> _consumers = [];
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the bus over a shared producer, the message and handler configurations and the scope factory.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="messages">The per-message topic configurations (command and event).</param>
    /// <param name="handlers">The per-handler consumer configurations (command handler and event subscriber).</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    public Bus(IProducer<Null, byte[]> producer, IEnumerable<IMessageConfiguration> messages, IEnumerable<IHandlerConfiguration> handlers, IServiceScopeFactory scopeFactory)
    {
        _producer = producer;
        _messages = messages.ToDictionary(configuration => configuration.MessageType);
        _handlers = handlers.ToList();
        _scopeFactory = scopeFactory;
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

    /// <inheritdoc />
    public Task Start(CancellationToken cancellationToken = default)
    {
        _cancellation = new CancellationTokenSource();

        foreach (IHandlerConfiguration configuration in _handlers)
        {
            MethodInfo dispatch = Dispatch(configuration);

            for (int consumer = 0; consumer < configuration.Consumers; consumer++)
            {
                _consumers.Add(Task.Run(() => Consume(configuration, dispatch, _cancellation.Token), CancellationToken.None));
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task Stop(CancellationToken cancellationToken = default)
    {
        if (_cancellation is null) return;

        _cancellation.Cancel();

        await Task.WhenAll(_consumers).WaitAsync(cancellationToken);

        _cancellation.Dispose();
        _cancellation = null;
        _consumers.Clear();
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

    /// <summary>
    /// One consumer loop: consume → dispatch → commit (the offset commit is the ack). A failed
    /// delivery is not committed; the resilience policy (retry / redelivery / error topic) is built
    /// in the next phase.
    /// </summary>
    private async Task Consume(IHandlerConfiguration configuration, MethodInfo dispatch, CancellationToken cancellationToken)
    {
        using IConsumer<Null, byte[]> consumer = new ConsumerBuilder<Null, byte[]>(configuration.ConsumerConfig).Build();

        consumer.Subscribe(configuration.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Null, byte[]> result = consumer.Consume(cancellationToken);

                await (Task)dispatch.Invoke(this, [configuration, result, cancellationToken])!;

                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful stop.
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task HandleCommand<TCommand>(IHandlerConfiguration configuration, ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
        where TCommand : Domain.Command
    {
        Transport transport = CreateTransport(result);
        TCommand message = JsonSerializer.Deserialize<TCommand>(result.Message.Value)!;

        CommandContext<TCommand> context = new(
            message,
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
            transport.GetStringList(TransportHeaders.AggregateDestinationAddresses),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

        using IServiceScope scope = _scopeFactory.CreateScope();

        ICommandHandler<TCommand, CommandContext<TCommand>, Transport> handler =
            (ICommandHandler<TCommand, CommandContext<TCommand>, Transport>)scope.ServiceProvider.GetRequiredService(configuration.HandlerType);

        await handler.Handle(context, cancellationToken);
    }

    private async Task HandleEvent<TEvent>(IHandlerConfiguration configuration, ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
        where TEvent : Domain.Event
    {
        Transport transport = CreateTransport(result);
        TEvent message = JsonSerializer.Deserialize<TEvent>(result.Message.Value)!;

        EventContext<TEvent> context = new(
            message,
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
            transport.GetStringList(TransportHeaders.AggregateDestinationAddresses),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

        using IServiceScope scope = _scopeFactory.CreateScope();

        IEventSubscriber<TEvent, EventContext<TEvent>, Transport> subscriber =
            (IEventSubscriber<TEvent, EventContext<TEvent>, Transport>)scope.ServiceProvider.GetRequiredService(configuration.HandlerType);

        await subscriber.Handle(context, cancellationToken);
    }

    private static MethodInfo Dispatch(IHandlerConfiguration configuration)
        => typeof(ICommand).IsAssignableFrom(configuration.MessageType)
            ? HandleCommandMethod.MakeGenericMethod(configuration.MessageType)
            : HandleEventMethod.MakeGenericMethod(configuration.MessageType);

    private static Transport CreateTransport(ConsumeResult<Null, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);

    private static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Bytes(Guid value) => value.ToByteArray();

    private static byte[] Bytes(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));
}
