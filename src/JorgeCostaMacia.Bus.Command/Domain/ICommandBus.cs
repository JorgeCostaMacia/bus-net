using JorgeCostaMacia.Bus.Domain.Buses;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>
/// Sends commands point-to-point. Bound to <see cref="ICommand"/>, so the compiler prevents sending
/// anything that is not a command through it.
/// </summary>
public interface ICommandBus : ISenderBus<ICommand> { }
