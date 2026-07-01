using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The Kafka command context a handler receives — implements
/// <see cref="ICommandContext{TCommand, TTransport}"/> for <see cref="KafkaTransport"/>, carrying the
/// command, the transport and the full read-only envelope. Three constructors: the full one
/// (<see cref="JsonConstructorAttribute"/>, for deserialization / replay), one that derives an
/// outbound message from an inbound context (propagating correlation and conversation), and one that
/// starts a brand-new flow.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public sealed record CommandContext<TCommand> : ICommandContext<TCommand, KafkaTransport>
    where TCommand : Command
{
    /// <summary>The transport this command arrived on (Kafka headers / offset / …).</summary>
    public KafkaTransport Transport { get; init; }

    /// <summary>The delivered command.</summary>
    public TCommand Message { get; init; }

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

    /// <summary>Destination addresses this command targets; empty means no filtering.</summary>
    public ImmutableList<string> AggregateDestinationAddresses { get; init; }

    /// <summary>Unique id of the inbound message (domain trace).</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Domain correlation id, propagated to messages sent from this handler.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC event-time of the inbound message.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>Number of in-process retry attempts made for this delivery.</summary>
    public int RetryCount { get; init; }

    /// <summary>Number of times this message has been redelivered by the transport.</summary>
    public int RedeliveryCount { get; init; }

    /// <summary>Full constructor — every value supplied. Used for deserialization and replay.</summary>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    /// <param name="message">The command payload.</param>
    /// <param name="messageId">Unique id of this message.</param>
    /// <param name="messageType">Logical type name of the message.</param>
    /// <param name="messageTypeUrn">URNs of the message type and its base types/interfaces.</param>
    /// <param name="messageDestinationAddress">Primary destination address.</param>
    /// <param name="messageOriginAddress">Primary origin address, when known.</param>
    /// <param name="messageOccurredAt">UTC time the message was created/sent.</param>
    /// <param name="conversationId">Conversation trace id.</param>
    /// <param name="conversationAddress">Address the conversation originated at.</param>
    /// <param name="conversationOccurredAt">UTC time the conversation began.</param>
    /// <param name="aggregateDestinationAddresses">Destination addresses this command targets.</param>
    /// <param name="aggregateId">Domain id of the inbound message.</param>
    /// <param name="aggregateCorrelationId">Domain correlation id.</param>
    /// <param name="aggregateOccurredAt">UTC event-time of the inbound message.</param>
    /// <param name="retryCount">In-process retry attempts.</param>
    /// <param name="redeliveryCount">Transport redeliveries.</param>
    [JsonConstructor]
    public CommandContext(KafkaTransport transport, TCommand message, Guid messageId, string messageType, ImmutableList<string> messageTypeUrn, string messageDestinationAddress, string? messageOriginAddress, DateTime messageOccurredAt, Guid conversationId, string conversationAddress, DateTime conversationOccurredAt, ImmutableList<string> aggregateDestinationAddresses, Guid aggregateId, Guid aggregateCorrelationId, DateTime aggregateOccurredAt, int retryCount, int redeliveryCount)
    {
        Transport = transport;
        Message = message;
        MessageId = messageId;
        MessageType = messageType;
        MessageTypeUrn = messageTypeUrn;
        MessageDestinationAddress = messageDestinationAddress;
        MessageOriginAddress = messageOriginAddress;
        MessageOccurredAt = messageOccurredAt;
        ConversationId = conversationId;
        ConversationAddress = conversationAddress;
        ConversationOccurredAt = conversationOccurredAt;
        AggregateDestinationAddresses = aggregateDestinationAddresses;
        AggregateId = aggregateId;
        AggregateCorrelationId = aggregateCorrelationId;
        AggregateOccurredAt = aggregateOccurredAt;
        RetryCount = retryCount;
        RedeliveryCount = redeliveryCount;
    }

    /// <summary>
    /// Derives an outbound command from an inbound context: keeps the conversation and the message's
    /// own domain trace, assigns fresh message identity and time, and re-stamps origin as unknown.
    /// </summary>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    /// <param name="message">The command payload.</param>
    /// <param name="messageDestinationAddress">Primary destination address.</param>
    /// <param name="conversationId">Conversation id inherited from the inbound context.</param>
    /// <param name="conversationAddress">Conversation origin address inherited from the inbound context.</param>
    /// <param name="conversationOccurredAt">Conversation start time inherited from the inbound context.</param>
    /// <param name="aggregateDestinationAddresses">Destination addresses, or <see langword="null"/> for none.</param>
    /// <param name="retryCount">Retry count, or <see langword="null"/> for zero.</param>
    /// <param name="redeliveryCount">Redelivery count, or <see langword="null"/> for zero.</param>
    public CommandContext(KafkaTransport transport, TCommand message, string messageDestinationAddress, Guid conversationId, string conversationAddress, DateTime conversationOccurredAt, ImmutableList<string>? aggregateDestinationAddresses, int? retryCount, int? redeliveryCount)
    {
        Transport = transport;
        Message = message;
        MessageId = JorgeCostaMacia.GuidFactory.Domain.GuidFactory.Create();
        MessageType = typeof(TCommand).FullName ?? typeof(TCommand).Name;
        MessageTypeUrn = JorgeCostaMacia.Bus.UrnFactory.Domain.UrnFactory.Create<TCommand>();
        MessageDestinationAddress = messageDestinationAddress;
        MessageOriginAddress = null;
        MessageOccurredAt = DateTime.UtcNow;
        ConversationId = conversationId;
        ConversationAddress = conversationAddress;
        ConversationOccurredAt = conversationOccurredAt;
        AggregateDestinationAddresses = aggregateDestinationAddresses?.ToImmutableList() ?? [];
        AggregateId = message.AggregateId;
        AggregateCorrelationId = message.AggregateCorrelationId;
        AggregateOccurredAt = message.AggregateOccurredAt;
        RetryCount = retryCount ?? 0;
        RedeliveryCount = redeliveryCount ?? 0;
    }

    /// <summary>
    /// Starts a brand-new flow: all identity and time are generated, and the conversation begins here
    /// (its id/address/time mirror this message). Domain trace comes from the message itself.
    /// </summary>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    /// <param name="message">The command payload.</param>
    /// <param name="messageDestinationAddress">Primary destination address.</param>
    /// <param name="aggregateDestinationAddresses">Destination addresses, or <see langword="null"/> for none.</param>
    public CommandContext(KafkaTransport transport, TCommand message, string messageDestinationAddress, ImmutableList<string>? aggregateDestinationAddresses)
    {
        Transport = transport;
        Message = message;
        MessageId = JorgeCostaMacia.GuidFactory.Domain.GuidFactory.Create();
        MessageType = typeof(TCommand).FullName ?? typeof(TCommand).Name;
        MessageTypeUrn = JorgeCostaMacia.Bus.UrnFactory.Domain.UrnFactory.Create<TCommand>();
        MessageDestinationAddress = messageDestinationAddress;
        MessageOriginAddress = null;
        MessageOccurredAt = DateTime.UtcNow;
        ConversationId = MessageId;
        ConversationAddress = messageDestinationAddress;
        ConversationOccurredAt = MessageOccurredAt;
        AggregateDestinationAddresses = message.AggregateDestinationAddresses ?? [];
        AggregateId = message.AggregateId;
        AggregateCorrelationId = message.AggregateCorrelationId;
        AggregateOccurredAt = message.AggregateOccurredAt;
        RetryCount = 0;
        RedeliveryCount = 0;
    }
}
