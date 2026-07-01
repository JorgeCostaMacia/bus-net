using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>Marker for a command handler, so command handlers can be recognised and registered.</summary>
public interface ICommandHandler : IHandler { }

/// <summary>
/// Handles a <typeparamref name="TCommand"/>, receiving a context of the exact shape
/// (<typeparamref name="TContext"/>) it needs — composed from the facets in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>.
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
/// <typeparam name="TContext">The context shape the handler requires for that command.</typeparam>
public interface ICommandHandler<TCommand, TContext> : ICommandHandler, IHandler<TCommand, TContext>
    where TCommand : ICommand
    where TContext : ICommandContext
{ }
