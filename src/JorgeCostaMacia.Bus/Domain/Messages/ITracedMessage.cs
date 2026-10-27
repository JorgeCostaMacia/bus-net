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
    /// UTC timestamp of when the event occurred — what happened, and when. How a consumer orders or
    /// reconciles messages with it, or with a version its own data carries, is the consumer's decision.
    /// </summary>
    DateTime AggregateOccurredAt { get; }
}
