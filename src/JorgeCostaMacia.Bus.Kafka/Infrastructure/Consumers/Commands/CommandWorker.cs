using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;

/// <summary>
/// The consumer hosting one command handler. Commands are point-to-point (one group), so the base
/// defaults apply: no consumer-side filtering — the command's <c>AggregateConsumers</c> is data for
/// the events it generates, not a delivery instruction. On a failure it hands the delivery to its
/// command error handler (the framework mechanics — retry ladder, error parking) over the context
/// already built, so the command deserialized once serves the handler and the error handler alike.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandWorker<TCommand, TCommandHandler> : ConsumerWorker<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    /// <summary>Creates the consumer over its inbound gate, the scope factory, the logger, the reachability tracker and its contract — the handler and its error and fault handlers are resolved per delivery from the scope.</summary>
    /// <param name="consumer">The inbound gate over the Kafka client — its settings and logging handlers already wired.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="health">The broker-reachability tracker — every consumed delivery reports the brokers up.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets.</param>
    public CommandWorker(
        IConsumer consumer,
        IServiceScopeFactory scopeFactory,
        ILogger<CommandWorker<TCommand, TCommandHandler>> logger,
        IHostApplicationLifetime lifetime,
        BusHealth health,
        string topic,
        string groupId)
        : base(consumer, scopeFactory, logger, lifetime, health, topic, groupId)
    {
    }

    /// <inheritdoc />
    protected override CommandContext<TCommand> CreateContext(ConsumeResult<Ignore, byte[]> result, Transport transport)
        => new(JsonSerializer.Deserialize<TCommand>(result.Message.Value, BusSerializer.Options) ?? throw new JsonException("The command body deserialized to null."), transport);

    /// <inheritdoc />
    protected override Task Handle(TCommandHandler handler, CommandContext<TCommand> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorResult> HandleError(IServiceProvider services, CommandContext<TCommand> context, Exception exception, CancellationToken cancellationToken)
    {
        CommandErrorHandlerBase<TCommand, TCommandHandler> errorHandler = services.GetRequiredService<CommandErrorHandlerBase<TCommand, TCommandHandler>>();

        await errorHandler.Handle(new CommandErrorContext<TCommand>(context.Message, context.Transport, exception), cancellationToken);

        return errorHandler.Result;
    }

    /// <inheritdoc />
    protected override async Task<FaultResult> HandleFault(IServiceProvider services, byte[] body, Transport transport, Exception exception, CancellationToken cancellationToken)
    {
        CommandFaultHandlerBase<TCommand, TCommandHandler> faultHandler = services.GetRequiredService<CommandFaultHandlerBase<TCommand, TCommandHandler>>();

        await faultHandler.Handle(CommandFaultContext.Create(body, transport, exception), cancellationToken);

        return faultHandler.Result;
    }
}
