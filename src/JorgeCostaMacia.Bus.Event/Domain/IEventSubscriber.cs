using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Event.Domain;

/// <summary>Marker for an event subscriber, so subscribers can be recognised and registered.</summary>
public interface IEventSubscriber : IHandler { }

/// <summary>
/// Subscribes to a <typeparamref name="TEvent"/>, receiving the exact context shape
/// (<typeparamref name="TContext"/>) it needs — the event context for
/// <typeparamref name="TTransport"/>, composed from the facets in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>. Concrete transports wrap this in an ergonomic base
/// (fixing the context and transport) so a subscriber declares only its event type.
/// </summary>
/// <typeparam name="TEvent">The event type this subscriber processes.</typeparam>
/// <typeparam name="TContext">The context shape the subscriber requires for that event.</typeparam>
/// <typeparam name="TTransport">The transport the event arrived on.</typeparam>
public interface IEventSubscriber<TEvent, TContext, TTransport> : IEventSubscriber, IHandler<TEvent, TContext>
    where TEvent : IEvent
    where TTransport : ITransport
    where TContext : IEventContext<TEvent, TTransport>
{ }
