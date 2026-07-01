using JorgeCostaMacia.Bus.Domain.Buses;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>
/// Sends commands point-to-point — plain, or correlated with an inbound context (to propagate
/// correlation when re-sending from inside a handler). Bound to <see cref="ICommand"/>, so the
/// compiler prevents sending anything that is not a command through it.
/// </summary>
public interface ICommandBus : ISenderBus<ICommand>, ISenderTracedBus<ICommand> { }
