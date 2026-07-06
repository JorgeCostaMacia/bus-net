using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A message that names the destinations it is meant for, so subscribers can filter it out
/// (consumer-side filtering) without processing messages addressed elsewhere.
/// </summary>
public interface IFilteredMessage : IMessage
{
    /// <summary>The consumers this message targets (e.g. consumer group ids); empty means "no filtering".</summary>
    ImmutableList<string> AggregateConsumers { get; }
}
