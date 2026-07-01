namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the inbound message's domain trace (id / correlation / timestamp) from
/// the transport header — so it can be read for correlation-propagation, logging or tracing without
/// deserializing the message body. Non-generic so any context can be used as a propagation source.
/// </summary>
public interface IAggregateTracedContext : IContext
{
    /// <summary>Unique id of the inbound message.</summary>
    Guid AggregateId { get; }

    /// <summary>Domain correlation id (caller-decided), propagated to messages sent from this handler.</summary>
    Guid AggregateCorrelationId { get; }

    /// <summary>UTC event-time of the inbound message.</summary>
    DateTime AggregateOccurredAt { get; }
}

/// <summary>The domain-trace envelope facet labelled with a specific inbound message type.</summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IAggregateTracedContext<TMessage> : IAggregateTracedContext
    where TMessage : IMessage
{ }
