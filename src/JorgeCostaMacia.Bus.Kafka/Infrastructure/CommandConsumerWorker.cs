using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one command handler. Commands are point-to-point (one group), so the base
/// defaults apply: no consumer-side filtering — the command's <c>AggregateConsumers</c> is data for
/// the events it generates, not a delivery instruction — and retries are requeued with the envelope
/// untouched.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandConsumerWorker<TCommand, TCommandHandler> : ConsumerWorker<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Domain.Command
    where TCommandHandler : class, ICommandHandler<TCommand, CommandContext<TCommand>, Transport>
{
    /// <summary>Creates the consumer over its ready-made Kafka builder, the shared producer, the scope factory, the logger and its custom policy.</summary>
    /// <param name="builder">The consumer builder, with the Kafka settings and logging handlers already wired.</param>
    /// <param name="producer">The shared Kafka producer, used to requeue failed deliveries.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries and retries.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets.</param>
    /// <param name="retryAttempts">Maximum retry attempts when handling fails (0 means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retries.</param>
    /// <param name="redeliveryIntervals">Delays between scheduled redeliveries (empty means no redeliveries).</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exception types excluded from redelivery.</param>
    public CommandConsumerWorker(
        ConsumerBuilder<Null, byte[]> builder,
        IProducer<Null, byte[]> producer,
        IServiceScopeFactory scopeFactory,
        ILogger<CommandConsumerWorker<TCommand, TCommandHandler>> logger,
        string topic,
        string groupId,
        int retryAttempts,
        ImmutableList<Type> retryExcludeExceptionTypes,
        ImmutableList<TimeSpan> redeliveryIntervals,
        ImmutableList<Type> redeliveryExcludeExceptionTypes)
        : base(builder, producer, scopeFactory, logger, topic, groupId, retryAttempts, retryExcludeExceptionTypes, redeliveryIntervals, redeliveryExcludeExceptionTypes) { }

    /// <inheritdoc />
    protected override CommandContext<TCommand> CreateContext(ConsumeResult<Null, byte[]> result, Transport transport)
        => new(
            JsonSerializer.Deserialize<TCommand>(result.Message.Value)!,
            transport,
            transport.GetGuid(TransportHeaders.MessageId),
            transport.GetString(TransportHeaders.MessageType),
            transport.GetStringList(TransportHeaders.MessageTypeUrn),
            transport.GetString(TransportHeaders.MessageDestinationAddress),
            transport.GetStringOrDefault(TransportHeaders.MessageOriginAddress),
            transport.GetDateTime(TransportHeaders.MessageOccurredAt),
            transport.GetGuid(TransportHeaders.ConversationId),
            transport.GetString(TransportHeaders.ConversationAddress),
            transport.GetDateTime(TransportHeaders.ConversationOccurredAt),
            transport.GetStringList(TransportHeaders.AggregateConsumers),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

    /// <inheritdoc />
    protected override Task Handle(TCommandHandler handler, CommandContext<TCommand> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);
}
