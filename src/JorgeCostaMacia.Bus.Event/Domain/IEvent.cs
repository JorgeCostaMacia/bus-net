using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.DomainEvent.Domain;

namespace JorgeCostaMacia.Bus.Event.Domain;

/// <summary>
/// Marker for an event — a domain fact that has already happened, published to every interested
/// subscriber (pub/sub). Being an <see cref="IDomainEvent"/> it fits an aggregate's event list; on
/// the bus it also carries messaging traceability (<see cref="ITracedMessage"/>) and consumer-side
/// filtering (<see cref="IFilteredMessage"/>).
/// </summary>
public interface IEvent : IDomainEvent, ITracedMessage, IFilteredMessage { }
