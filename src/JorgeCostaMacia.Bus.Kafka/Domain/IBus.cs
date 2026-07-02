using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The bus — the single entry point for a service: it sends commands (<see cref="ICommandBus"/>)
/// and publishes events (<see cref="IEventBus"/>); the per-message configuration is held and managed
/// inside it. The consume side lives in the worker, hosted in the application lifecycle — nothing to
/// start or stop by hand.
/// </summary>
public interface IBus : ICommandBus, IEventBus { }
