using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>
/// Marker for a command — a message that requests a single action, delivered point-to-point to one
/// handler. Carries traceability (<see cref="ITracedMessage"/>) and filtering
/// (<see cref="IFilteredMessage"/>) metadata.
/// </summary>
public interface ICommand : ITracedMessage, IFilteredMessage { }
