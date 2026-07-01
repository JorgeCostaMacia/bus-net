using JorgeCostaMacia.Bus.Domain.Buses;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>
/// Sends commands point-to-point — plain (new conversation), or continuing from an inbound transport
/// (propagating its envelope) when re-sending from inside a handler. Bound to <see cref="ICommand"/>,
/// so the compiler prevents sending anything that is not a command.
/// </summary>
public interface ICommandBus : ISenderBus<ICommand> { }
