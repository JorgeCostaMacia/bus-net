using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Commands;

/// <summary>
/// The Kafka command context a handler receives — composes the envelope facets over
/// <see cref="Transport"/> (all but the filtering one: commands are point-to-point and never
/// filtered), carrying only the command and the transport: every envelope property reads straight
/// from the transport's headers on access, nothing duplicated in memory. Built by the consumer,
/// which deserializes the message once per delivery; the <b>outbound</b> envelope (new flow / correlated) is computed by
/// the bus when producing, not here.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public record CommandContext<TCommand> :
    IMessageContext<TCommand>,
    ITransportContext<Transport>,
    ITracedContext,
    IAggregateTracedContext,
    IConversationContext,
    IResilientContext,
    IHostContext
    where TCommand : Command
{
    /// <summary>The delivered command.</summary>
    public TCommand Message { get; init; }

    /// <summary>The transport this command arrived on (Kafka headers / offset / …).</summary>
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

    /// <summary>Builds the context over the delivered command and its transport.</summary>
    /// <param name="message">The command payload.</param>
    /// <param name="transport">The Kafka transport for this delivery.</param>
    public CommandContext(TCommand message, Transport transport)
    {
        Message = message;
        Transport = transport;
    }

}
