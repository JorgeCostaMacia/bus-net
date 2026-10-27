namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A message carrying traceability metadata for correlation and causality across the bus.
/// </summary>
public interface ITracedMessage : IMessage
{
    /// <summary>Unique identifier of this message instance — what a consumer deduplicates a redelivery by.</summary>
    Guid AggregateId { get; }

    /// <summary>
    /// Identifier shared by every message in the same workflow: the thread a trace follows across services,
    /// and the key a saga or process manager can correlate its steps by.
    /// </summary>
    Guid AggregateCorrelationId { get; }

    /// <summary>
    /// UTC timestamp of when the event occurred. A consumer can use it to order its messages — the newer
    /// occurrence wins, so a stale or reprocessed one never overwrites a newer state — or compare a version
    /// its own data carries instead; which one is the consumer's decision.
    /// </summary>
    DateTime AggregateOccurredAt { get; }
}
