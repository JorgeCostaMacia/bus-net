using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>
/// Base implementation for commands: an immutable <see langword="record"/> carrying traceability
/// metadata (id / correlation / UTC timestamp) and optional destination addresses, defaulting the
/// id via JorgeCostaMacia.GuidFactory. Concrete commands forward to it with <c>: base(...)</c>.
/// </summary>
public abstract record Command : ICommand
{
    /// <summary>Unique identifier of this command instance.</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Correlation identifier shared by every message in the same workflow.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC timestamp of when the command was issued.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>Destination addresses this command targets; empty means no filtering.</summary>
    public ImmutableList<string> AggregateDestinationAddresses { get; init; }

    /// <summary>
    /// Initializes the traceability metadata, generating defaults when a value is not supplied: the
    /// id via a time-ordered UUIDv7, the correlation id defaulting to the id, and the timestamp to
    /// <see cref="DateTime.UtcNow"/>.
    /// </summary>
    /// <param name="aggregateId">The command id, or <see langword="null"/> to generate one.</param>
    /// <param name="aggregateCorrelationId">The correlation id, or <see langword="null"/> to default to the id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp, or <see langword="null"/> for now.</param>
    /// <param name="aggregateDestinationAddresses">The destination addresses, or <see langword="null"/> for none.</param>
    protected Command(Guid? aggregateId, Guid? aggregateCorrelationId, DateTime? aggregateOccurredAt, IEnumerable<string>? aggregateDestinationAddresses)
    {
        AggregateId = aggregateId ?? JorgeCostaMacia.GuidFactory.Domain.GuidFactory.Create();
        AggregateCorrelationId = aggregateCorrelationId ?? AggregateId;
        AggregateOccurredAt = aggregateOccurredAt ?? DateTime.UtcNow;
        AggregateDestinationAddresses = aggregateDestinationAddresses?.ToImmutableList() ?? [];
    }
}
