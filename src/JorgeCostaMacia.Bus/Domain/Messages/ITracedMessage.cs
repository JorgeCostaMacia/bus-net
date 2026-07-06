namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A message carrying traceability metadata for correlation and causality across the bus.
/// </summary>
public interface ITracedMessage : IMessage
{
    /// <summary>Unique identifier of this message instance.</summary>
    Guid AggregateId { get; }

    /// <summary>Identifier shared by every message in the same workflow, used for correlation.</summary>
    Guid AggregateCorrelationId { get; }

    /// <summary>
    /// UTC timestamp of when the message was created — the event-time used for last-writer-wins
    /// conflict resolution (so out-of-order or reprocessed messages never overwrite a newer one).
    /// </summary>
    DateTime AggregateOccurredAt { get; }
}
