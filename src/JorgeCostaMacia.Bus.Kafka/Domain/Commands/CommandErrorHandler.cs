using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Commands;

/// <summary>
/// Ergonomic base for a command's error handler — the app's own error management (log, compensate,
/// notify, decide), invoked when the command's handler fails terminally and the delivery parks to
/// <c>.error</c>: closes the <see cref="IHandler{TMessage, TContext}"/> contract over
/// <see cref="CommandErrorContext{TCommand}"/>, so a concrete error handler declares only its
/// command type and gets fully-typed access to the failed command, its envelope and the exception.
/// </summary>
/// <typeparam name="TCommand">The command type whose terminal failures this handler manages.</typeparam>
internal abstract class CommandErrorHandler<TCommand> : IHandler<TCommand, CommandErrorContext<TCommand>>
    where TCommand : Command
{
    /// <summary>
    /// How the handler left the delivery — set as it runs, read by the consumer afterwards (since
    /// <see cref="Handle"/> returns <see cref="Task"/> by contract and cannot report it). Defaults to
    /// <see cref="ErrorHandlerResult.Unhandled"/> until the handler decides.
    /// </summary>
    public ErrorHandlerResult Result { get; protected set; }

    /// <summary>Handles the terminal failure of a command delivery.</summary>
    /// <param name="context">The delivery's error context — the failed command, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(CommandErrorContext<TCommand> context, CancellationToken cancellationToken = default);
}
