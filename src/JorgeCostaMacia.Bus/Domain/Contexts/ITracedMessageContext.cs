using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the messaging-level trace assigned by the transport: the message's own
/// id, type and type URNs (for polymorphic routing and versioning), origin/destination address and
/// sent time. Distinct from <see cref="IAggregateTracedMessageContext"/>, which carries the
/// caller-set domain trace. Non-generic so it can be read without knowing the message type.
/// </summary>
public interface ITracedMessageContext : IMessageContext
{
    /// <summary>Unique id of this message, assigned by the messaging layer.</summary>
    Guid MessageId { get; }

    /// <summary>Logical type name of the message.</summary>
    string MessageType { get; }

    /// <summary>
    /// Ordered URNs of the message type and its base types/interfaces, enabling polymorphic routing
    /// and versioning (e.g. subscribing to <c>IEvent</c> to receive every domain event).
    /// </summary>
    ImmutableList<string> MessageTypeUrn { get; }

    /// <summary>Primary destination address (topic / exchange / queue).</summary>
    string MessageDestinationAddress { get; }

    /// <summary>Primary origin address (topic / exchange / queue), when known.</summary>
    string? MessageOriginAddress { get; }

    /// <summary>UTC time when the message was created/sent.</summary>
    DateTime MessageOccurredAt { get; }
}

/// <summary>The messaging-trace envelope facet bound to a specific inbound message type.</summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface ITracedMessageContext<TMessage> : ITracedMessageContext, IMessageContext<TMessage>
    where TMessage : IMessage
{ }
