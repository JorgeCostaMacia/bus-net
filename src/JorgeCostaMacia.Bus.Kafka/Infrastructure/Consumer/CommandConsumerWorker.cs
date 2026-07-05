using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumer;

/// <summary>
/// The consumer hosting one command handler. Commands are point-to-point (one group), so the base
/// defaults apply: no consumer-side filtering — the command's <c>AggregateConsumers</c> is data for
/// the events it generates, not a delivery instruction. On a failure it hands the delivery to its
/// command error handler (the framework mechanics — retry ladder, error parking) over the context
/// already built, so the command deserialized once serves the handler and the error handler alike.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandConsumerWorker<TCommand, TCommandHandler> : ConsumerWorker<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : class, IHandler<TCommand, CommandContext<TCommand>>
{
    private readonly Domain.Commands.CommandErrorHandler<TCommand> _errorHandler;

    /// <summary>Creates the consumer over its ready-made Kafka builder, its error and fault handlers, the scope factory, the logger and its contract.</summary>
    /// <param name="builder">The consumer builder, with the Kafka settings and logging handlers already wired.</param>
    /// <param name="errorHandler">The command's error handler — the framework mechanics deciding a failed delivery's outcome.</param>
    /// <param name="faultHandler">The fault handler parking broken deliveries — and the relay when the error handler cannot cope.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets.</param>
    public CommandConsumerWorker(
        ConsumerBuilder<Ignore, byte[]> builder,
        Domain.Commands.CommandErrorHandler<TCommand> errorHandler,
        Domain.Faults.FaultHandler faultHandler,
        IServiceScopeFactory scopeFactory,
        ILogger<CommandConsumerWorker<TCommand, TCommandHandler>> logger,
        IHostApplicationLifetime lifetime,
        string topic,
        string groupId)
        : base(builder, faultHandler, scopeFactory, logger, lifetime, topic, groupId)
        => _errorHandler = errorHandler;

    /// <inheritdoc />
    protected override CommandContext<TCommand> CreateContext(ConsumeResult<Ignore, byte[]> result, Transport transport)
        => new(JsonSerializer.Deserialize<TCommand>(result.Message.Value)!, transport);

    /// <inheritdoc />
    protected override Task Handle(TCommandHandler handler, CommandContext<TCommand> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorHandlerResult> HandleError(CommandContext<TCommand> context, Exception exception, CancellationToken cancellationToken)
    {
        await _errorHandler.Handle(new CommandErrorContext<TCommand>(context.Message, context.Transport, exception), cancellationToken);

        return _errorHandler.Result;
    }
}
