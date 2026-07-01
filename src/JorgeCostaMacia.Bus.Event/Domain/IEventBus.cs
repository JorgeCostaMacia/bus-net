using JorgeCostaMacia.Bus.Domain.Buses;

namespace JorgeCostaMacia.Bus.Event.Domain;

/// <summary>
/// Publishes events to every interested subscriber (pub/sub) — plain, or correlated with an inbound
/// context (to propagate correlation when publishing from inside a handler). Bound to
/// <see cref="IEvent"/>, so the compiler prevents publishing anything that is not an event through it.
/// </summary>
public interface IEventBus : IPublisherBus<IEvent>, IPublisherTracedBus<IEvent> { }
