using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Event.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The Kafka event context a subscriber receives — implements
/// <see cref="IEventContext{TEvent, TTransport}"/> for <see cref="Transport"/>, carrying the event,
/// the transport and the full read-only envelope. Built by the consumer from the delivered message
/// and its headers; the <b>outbound</b> envelope (new flow / correlated) is computed by the bus when
/// producing, not here.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
public sealed record EventContext<TEvent> : IEventContext<TEvent, Transport>
    where TEvent : Event
{
    /// <summary>The delivered event.</summary>
    public TEvent Message { get; init; }

    /// <summary>The transport this event arrived on (Kafka headers / offset / …).</summary>
    public Transport Transport { get; init; }

    /// <summary>Unique id of this message, assigned by the messaging layer.</summary>
    public Guid MessageId { get; init; }

    /// <summary>Logical type name of the message.</summary>
    public string MessageType { get; init; }

    /// <summary>Ordered URNs of the message type and its base types/interfaces (polymorphic routing / versioning).</summary>
    public ImmutableList<string> MessageTypeUrn { get; init; }

    /// <summary>Primary destination address (topic).</summary>
    public string MessageDestinationAddress { get; init; }

    /// <summary>Primary origin address (topic) the message came from, when known.</summary>
    public string? MessageOriginAddress { get; init; }

    /// <summary>UTC time when the message was created/sent.</summary>
    public DateTime MessageOccurredAt { get; init; }

    /// <summary>Conversation trace id, shared by the whole chain; equals the first message's id.</summary>
    public Guid ConversationId { get; init; }

    /// <summary>Address where the conversation originated — the first message's destination.</summary>
    public string ConversationAddress { get; init; }

    /// <summary>UTC time when the conversation began.</summary>
    public DateTime ConversationOccurredAt { get; init; }

    /// <summary>The consumers this event targets (e.g. consumer group ids); empty means no filtering.</summary>
    public ImmutableList<string> AggregateConsumers { get; init; }

    /// <summary>Unique id of the inbound message (domain trace).</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Domain correlation id, propagated to messages sent from this handler.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC event-time of the inbound message.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>Number of times this message has been retried (immediate or scheduled).</summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Builds the context with every envelope value supplied — used by the consumer to reconstruct
    /// it from the delivered message and its headers.
    /// </summary>
    /// <param name="message">The event payload.</param>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    /// <param name="messageId">Unique id of this message.</param>
    /// <param name="messageType">Logical type name of the message.</param>
    /// <param name="messageTypeUrn">URNs of the message type and its base types/interfaces.</param>
    /// <param name="messageDestinationAddress">Primary destination address.</param>
    /// <param name="messageOriginAddress">Primary origin address, when known.</param>
    /// <param name="messageOccurredAt">UTC time the message was created/sent.</param>
    /// <param name="conversationId">Conversation trace id.</param>
    /// <param name="conversationAddress">Address the conversation originated at.</param>
    /// <param name="conversationOccurredAt">UTC time the conversation began.</param>
    /// <param name="aggregateConsumers">The consumers this event targets.</param>
    /// <param name="aggregateId">Domain id of the inbound message.</param>
    /// <param name="aggregateCorrelationId">Domain correlation id.</param>
    /// <param name="aggregateOccurredAt">UTC event-time of the inbound message.</param>
    /// <param name="retryCount">Retries of this message (immediate or scheduled).</param>
    public EventContext(TEvent message, Transport transport, Guid messageId, string messageType, ImmutableList<string> messageTypeUrn, string messageDestinationAddress, string? messageOriginAddress, DateTime messageOccurredAt, Guid conversationId, string conversationAddress, DateTime conversationOccurredAt, ImmutableList<string> aggregateConsumers, Guid aggregateId, Guid aggregateCorrelationId, DateTime aggregateOccurredAt, int retryCount)
    {
        Message = message;
        Transport = transport;
        MessageId = messageId;
        MessageType = messageType;
        MessageTypeUrn = messageTypeUrn;
        MessageDestinationAddress = messageDestinationAddress;
        MessageOriginAddress = messageOriginAddress;
        MessageOccurredAt = messageOccurredAt;
        ConversationId = conversationId;
        ConversationAddress = conversationAddress;
        ConversationOccurredAt = conversationOccurredAt;
        AggregateConsumers = aggregateConsumers;
        AggregateId = aggregateId;
        AggregateCorrelationId = aggregateCorrelationId;
        AggregateOccurredAt = aggregateOccurredAt;
        RetryCount = retryCount;
    }
}
