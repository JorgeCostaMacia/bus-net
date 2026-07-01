using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Event.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Base implementation for events on the Kafka bus: an immutable <see langword="record"/> carrying
/// traceability metadata (id / correlation / UTC timestamp) and optional destination addresses,
/// defaulting the id via JorgeCostaMacia.GuidFactory. Concrete events forward to it with
/// <c>: base(...)</c>. Implements the transport-agnostic <see cref="IEvent"/> contract (and thus
/// <c>IDomainEvent</c>), so it fits an aggregate's event list.
/// </summary>
public abstract record Event : IEvent
{
    /// <summary>Unique identifier of this event instance.</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Correlation identifier shared by every message in the same workflow.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC timestamp of when the event occurred.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>Destination addresses this event targets; empty means no filtering.</summary>
    public ImmutableList<string> AggregateDestinationAddresses { get; init; }

    /// <summary>
    /// Initializes the traceability metadata, generating defaults when a value is not supplied: the
    /// id via a time-ordered UUIDv7, the correlation id defaulting to the id, and the timestamp to
    /// <see cref="DateTime.UtcNow"/>.
    /// </summary>
    /// <param name="aggregateId">The event id, or <see langword="null"/> to generate one.</param>
    /// <param name="aggregateCorrelationId">The correlation id, or <see langword="null"/> to default to the id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp, or <see langword="null"/> for now.</param>
    /// <param name="aggregateDestinationAddresses">The destination addresses, or <see langword="null"/> for none.</param>
    protected Event(Guid? aggregateId, Guid? aggregateCorrelationId, DateTime? aggregateOccurredAt, IEnumerable<string>? aggregateDestinationAddresses)
    {
        AggregateId = aggregateId ?? JorgeCostaMacia.GuidFactory.Domain.GuidFactory.Create();
        AggregateCorrelationId = aggregateCorrelationId ?? AggregateId;
        AggregateOccurredAt = aggregateOccurredAt ?? DateTime.UtcNow;
        AggregateDestinationAddresses = aggregateDestinationAddresses?.ToImmutableList() ?? [];
    }
}
