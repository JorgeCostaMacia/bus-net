using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Events;

/// <summary>
/// The Kafka event context a subscriber receives — composes every envelope facet over
/// <see cref="Transport"/>, carrying only the event and the transport: every envelope property
/// reads straight from the transport's headers on access, nothing duplicated in memory. Built by
/// the consumer from the delivered message; the <b>outbound</b> envelope (new flow / correlated)
/// is computed by the bus when producing, not here.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
public record EventContext<TEvent> :
    IMessageContext<TEvent>,
    ITransportContext<Transport>,
    ITracedContext,
    IAggregateTracedContext,
    IAggregateFilteredContext,
    IConversationContext,
    IResilientContext,
    IHostContext
    where TEvent : Event
{
    /// <summary>The delivered event.</summary>
    public TEvent Message { get; init; }

    /// <summary>The transport this event arrived on (Kafka headers / offset / …).</summary>
    public Transport Transport { get; init; }

    /// <summary>Unique id of this message, assigned by the messaging layer.</summary>
    public Guid MessageId => Transport.GetGuid(TransportHeaders.MessageId);

    /// <summary>Logical type name of the message.</summary>
    public string MessageType => Transport.GetString(TransportHeaders.MessageType);

    /// <summary>Ordered URNs of the message type and its base types/interfaces (polymorphic routing / versioning).</summary>
    public ImmutableList<string> MessageTypeUrn => Transport.GetStringList(TransportHeaders.MessageTypeUrn);

    /// <summary>Primary destination address (topic).</summary>
    public string MessageDestinationAddress => Transport.GetString(TransportHeaders.MessageDestinationAddress);

    /// <summary>Primary origin address (topic) the message came from, when known.</summary>
    public string? MessageOriginAddress => Transport.GetStringOrDefault(TransportHeaders.MessageOriginAddress);

    /// <summary>UTC time when the message was created/sent.</summary>
    public DateTime MessageOccurredAt => Transport.GetDateTime(TransportHeaders.MessageOccurredAt);

    /// <summary>Conversation trace id, shared by the whole chain; equals the first message's id.</summary>
    public Guid ConversationId => Transport.GetGuid(TransportHeaders.ConversationId);

    /// <summary>Address where the conversation originated — the first message's destination.</summary>
    public string ConversationAddress => Transport.GetString(TransportHeaders.ConversationAddress);

    /// <summary>UTC time when the conversation began.</summary>
    public DateTime ConversationOccurredAt => Transport.GetDateTime(TransportHeaders.ConversationOccurredAt);

    /// <summary>The consumers this event targets (e.g. consumer group ids); empty means no filtering.</summary>
    public ImmutableList<string> AggregateConsumers => Transport.GetStringList(TransportHeaders.AggregateConsumers);

    /// <summary>Unique id of the inbound message (domain trace).</summary>
    public Guid AggregateId => Transport.GetGuid(TransportHeaders.AggregateId);

    /// <summary>Domain correlation id, propagated to messages sent from this handler.</summary>
    public Guid AggregateCorrelationId => Transport.GetGuid(TransportHeaders.AggregateCorrelationId);

    /// <summary>UTC event-time of the inbound message.</summary>
    public DateTime AggregateOccurredAt => Transport.GetDateTime(TransportHeaders.AggregateOccurredAt);

    /// <summary>Number of times this message has been retried (immediate or scheduled).</summary>
    public int RetryCount => Transport.GetInt(TransportHeaders.RetryCount);

    /// <summary>The machine (host) name that produced the message.</summary>
    public string HostMachineName => Transport.GetString(TransportHeaders.HostMachineName);

    /// <summary>The entry assembly's simple name of the producing host.</summary>
    public string HostAssembly => Transport.GetString(TransportHeaders.HostAssembly);

    /// <summary>The entry assembly's version of the producing host.</summary>
    public string HostAssemblyVersion => Transport.GetString(TransportHeaders.HostAssemblyVersion);

    /// <summary>The .NET runtime version of the producing host.</summary>
    public string HostFrameworkVersion => Transport.GetString(TransportHeaders.HostFrameworkVersion);

    /// <summary>The bus library version of the producing host.</summary>
    public string HostBusVersion => Transport.GetString(TransportHeaders.HostBusVersion);

    /// <summary>The operating system version of the producing host.</summary>
    public string HostOperatingSystemVersion => Transport.GetString(TransportHeaders.HostOperatingSystemVersion);

    /// <summary>Builds the context over the delivered event and its transport.</summary>
    /// <param name="message">The event payload.</param>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    public EventContext(TEvent message, Transport transport)
    {
        Message = message;
        Transport = transport;
    }

}
