using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Commands;

/// <summary>
/// Ergonomic base for a Kafka command handler: closes the <see cref="IHandler{TMessage, TContext}"/>
/// contract over <see cref="CommandContext{TCommand}"/>, so a concrete handler declares only its
/// command type and gets fully-typed access to the message, transport and envelope.
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
public abstract class CommandHandler<TCommand> : IHandler<TCommand, CommandContext<TCommand>>
    where TCommand : Command
{
    /// <summary>Handles the delivered command.</summary>
    /// <param name="context">The delivery's context — command, transport and read-only envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(CommandContext<TCommand> context, CancellationToken cancellationToken = default);
}
