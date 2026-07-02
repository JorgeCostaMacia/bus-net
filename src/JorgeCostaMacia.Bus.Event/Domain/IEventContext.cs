using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Event.Domain;

/// <summary>Marker for an event subscriber context.</summary>
public interface IEventContext : IContext { }

/// <summary>
/// The context an event subscriber receives — the glue that binds the whole delivery together: the
/// <typeparamref name="TEvent"/> and the <typeparamref name="TTransport"/> it arrived on, plus the
/// read-only envelope facets (messaging trace, domain trace, target consumers, conversation and
/// resilience). <typeparamref name="TTransport"/> is bound by the transport (e.g. a Kafka transport),
/// so it never leaks onto the subscriber unless it asks for it.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
/// <typeparam name="TTransport">The transport the event arrived on.</typeparam>
public interface IEventContext<TEvent, TTransport>
    : IEventContext,
      IMessageContext<TEvent>,
      ITransportContext<TTransport>,
      ITracedContext,
      IAggregateTracedContext,
      IAggregateFilteredContext,
      IConversationContext,
      IResilientContext
    where TEvent : IEvent
    where TTransport : ITransport
{ }
