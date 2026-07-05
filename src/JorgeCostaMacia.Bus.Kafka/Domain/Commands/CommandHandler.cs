using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Commands;

/// <summary>
/// Ergonomic base for a Kafka command handler: closes the <see cref="IHandler{TMessage, TContext}"/>
/// contract over <see cref="CommandContext{TCommand}"/>, so a concrete handler declares only its
/// command type and gets fully-typed access to the message, transport and envelope.
/// <para>
/// Delivery is <b>at-least-once</b> and unordered: a handler may run more than once for the same
/// command (a retry re-produces it, and offsets commit on an interval so a crash reprocesses the last
/// few), and messages carry no cross-partition order. Handlers must therefore be <b>idempotent</b> —
/// deduplicate by the message's id or reconcile by its timestamp (last-writer-wins), never assume a
/// once-only, in-order call.
/// </para>
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
