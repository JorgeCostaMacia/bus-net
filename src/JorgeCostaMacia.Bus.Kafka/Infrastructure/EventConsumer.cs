using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one event subscriber. Events are pub/sub (one group per subscriber), so it
/// overrides the base's pub/sub hooks: consumer-side filtering (deliveries targeting other consumers
/// are skipped and acked, straight off the raw header) and retry targeting (the requeue is stamped
/// with this group only — the other subscriber groups already handled the original and filter the
/// retry out; the user's original targeting stays in the message body).
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventConsumer<TEvent, TEventSubscriber> : Consumer<EventContext<TEvent>, TEventSubscriber>
    where TEvent : Domain.Event
    where TEventSubscriber : class, IEventSubscriber<TEvent, EventContext<TEvent>, Transport>
{
    /// <summary>Creates the consumer over its subscriber configuration, the shared producer, the scope factory and the logger.</summary>
    /// <param name="configuration">The subscriber's consumer configuration.</param>
    /// <param name="producer">The shared Kafka producer, used to requeue failed deliveries.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for consumer errors, internal Kafka logs and retries.</param>
    public EventConsumer(ConsumerConfiguration configuration, IProducer<Null, byte[]> producer, IServiceScopeFactory scopeFactory, ILogger<EventConsumer<TEvent, TEventSubscriber>> logger)
        : base(configuration, producer, scopeFactory, logger) { }

    /// <inheritdoc />
    protected override EventContext<TEvent> CreateContext(ConsumeResult<Null, byte[]> result, Transport transport)
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

    /// <inheritdoc />
    protected override Task Handle(TEventSubscriber handler, EventContext<TEvent> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);

    /// <summary>
    /// Consumer-side filtering: when the message targets specific consumers
    /// (<c>AggregateConsumers</c> header non-empty) and this group is not among them, the delivery is
    /// skipped (and acked) without deserializing the body.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected override bool Filtered(ConsumeResult<Null, byte[]> result)
    {
        if (!result.Message.Headers.TryGetLastBytes(TransportHeaders.AggregateConsumers, out byte[] header)) return false;

        string consumers = Encoding.UTF8.GetString(header);

        if (string.IsNullOrWhiteSpace(consumers)) return false;

        return !consumers
            .Split(',')
            .Select(consumer => consumer.Trim())
            .Contains(Configuration.GroupId);
    }

    /// <summary>
    /// Targets the requeued retry to this group only — the other subscriber groups already handled
    /// the original and filter the retry out; the user's original targeting stays in the message body.
    /// </summary>
    /// <param name="headers">The requeued delivery's headers.</param>
    protected override void Target(Headers headers)
        => Restamp(headers, TransportHeaders.AggregateConsumers, Encoding.UTF8.GetBytes(Configuration.GroupId));
}
