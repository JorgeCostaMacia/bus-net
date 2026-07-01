using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the inbound message's destination addresses from the transport header,
/// so a worker can apply consumer-side filtering (discard messages not addressed to it) without
/// deserializing the body — keeping the filtering invisible to the handler.
/// </summary>
public interface IAggregateFilteredContext : IContext
{
    /// <summary>The destination addresses this message targets; empty means "no filtering".</summary>
    ImmutableList<string> AggregateDestinationAddresses { get; }
}

/// <summary>The filtered envelope facet labelled with a specific inbound message type.</summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IAggregateFilteredContext<TMessage> : IAggregateFilteredContext
    where TMessage : IMessage
{ }
