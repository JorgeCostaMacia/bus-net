using JorgeCostaMacia.Bus.Domain.Buses;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The bus — the single entry point for a service: it sends commands
/// (<see cref="ISenderBus{TMessage}"/> over <see cref="Command"/>) and publishes events
/// (<see cref="IPublisherBus{TMessage}"/> over <see cref="Event"/>); the per-message configuration is
/// held and managed inside it. The consume side lives in the worker, hosted in the application
/// lifecycle — nothing to start or stop by hand.
/// </summary>
public interface IBus : ISenderBus<Command>, IPublisherBus<Event> { }
