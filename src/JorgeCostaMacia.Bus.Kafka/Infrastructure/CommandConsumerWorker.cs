using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one command handler. Commands are point-to-point (one group), so the base
/// defaults apply: no consumer-side filtering — the command's <c>AggregateConsumers</c> is data for
/// the events it generates, not a delivery instruction — and retries are requeued with the
/// envelope untouched.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandConsumerWorker<TCommand, TCommandHandler> : ConsumerWorker<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : class, IHandler<TCommand, CommandContext<TCommand>>
{
    /// <summary>Creates the consumer over its ready-made Kafka builder, its failure policy, the scope factory, the logger and its contract.</summary>
    /// <param name="builder">The consumer builder, with the Kafka settings and logging handlers already wired.</param>
    /// <param name="errorHandler">The failure policy deciding a failed delivery's outcome — retry ladder, retry scheduler, error topic.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets.</param>
    public CommandConsumerWorker(
        ConsumerBuilder<Ignore, byte[]> builder,
        ConsumerErrorHandler errorHandler,
        IServiceScopeFactory scopeFactory,
        ILogger<CommandConsumerWorker<TCommand, TCommandHandler>> logger,
        IHostApplicationLifetime lifetime,
        string topic,
        string groupId)
        : base(builder, errorHandler, scopeFactory, logger, lifetime, topic, groupId) { }

    /// <inheritdoc />
    protected override CommandContext<TCommand> CreateContext(ConsumeResult<Ignore, byte[]> result, Transport transport)
        => CommandContext<TCommand>.Create(result.Message.Value, transport);

    /// <inheritdoc />
    protected override Task Handle(TCommandHandler handler, CommandContext<TCommand> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);
}
