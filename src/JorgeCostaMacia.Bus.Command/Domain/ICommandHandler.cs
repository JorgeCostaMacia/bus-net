using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>Marker for a command handler, so command handlers can be recognised and registered.</summary>
public interface ICommandHandler : IHandler { }

/// <summary>
/// Handles a <typeparamref name="TCommand"/>, receiving the exact context shape
/// (<typeparamref name="TContext"/>) it needs — the command context for
/// <typeparamref name="TTransport"/>, composed from the facets in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>. Concrete transports wrap this in an ergonomic base
/// (fixing the context and transport) so a handler declares only its command type.
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
/// <typeparam name="TContext">The context shape the handler requires for that command.</typeparam>
/// <typeparam name="TTransport">The transport the command arrived on.</typeparam>
public interface ICommandHandler<TCommand, TContext, TTransport> : ICommandHandler, IHandler<TCommand, TContext>
    where TCommand : ICommand
    where TTransport : ITransport
    where TContext : ICommandContext<TCommand, TTransport>
{ }
