using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Ergonomic base for a Kafka command handler: fixes the context
/// (<see cref="CommandContext{TCommand}"/>) and the transport (<see cref="KafkaTransport"/>), so a
/// concrete handler declares only its command type and gets fully-typed access to the message,
/// transport and envelope.
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
public abstract class CommandHandler<TCommand> : ICommandHandler<TCommand, CommandContext<TCommand>, KafkaTransport>
    where TCommand : Command
{
    /// <summary>Handles the delivered command.</summary>
    /// <param name="context">The command context — message, transport and read-only envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(CommandContext<TCommand> context, CancellationToken cancellationToken = default);
}
