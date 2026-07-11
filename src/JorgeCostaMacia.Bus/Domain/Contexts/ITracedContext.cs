namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the messaging-level trace assigned by the transport: the message's own
/// id, type, origin/destination address and sent time. Distinct from
/// <see cref="IAggregateTracedContext"/>, which carries the caller-set domain trace.
/// </summary>
public interface ITracedContext : IContext
{
    /// <summary>Unique id of this message, assigned by the messaging layer.</summary>
    Guid MessageId { get; }

    /// <summary>Logical type name of the message.</summary>
    string MessageType { get; }

    /// <summary>Primary destination address (topic / exchange / queue).</summary>
    string MessageDestinationAddress { get; }

    /// <summary>Primary origin address (topic / exchange / queue), when known.</summary>
    string? MessageOriginAddress { get; }

    /// <summary>UTC time when the message was created/sent.</summary>
    DateTime MessageOccurredAt { get; }
}
