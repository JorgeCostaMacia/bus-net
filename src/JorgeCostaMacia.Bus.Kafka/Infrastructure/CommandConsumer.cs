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
internal sealed class CommandConsumer<TCommand, TCommandHandler> : Consumer<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Domain.Command
    where TCommandHandler : class, ICommandHandler<TCommand, CommandContext<TCommand>, Transport>
{
    /// <summary>Creates the consumer over its custom and Kafka configurations, the shared producer, the scope factory and the logger.</summary>
    /// <param name="configuration">The handler's custom configuration (topic, group id, resilience policy).</param>
    /// <param name="consumerConfig">The Kafka consumer configuration composed for this group.</param>
    /// <param name="producer">The shared Kafka producer, used to requeue failed deliveries.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for consumer errors, internal Kafka logs and retries.</param>
    public CommandConsumer(HandlerConfiguration configuration, ConsumerConfig consumerConfig, IProducer<Null, byte[]> producer, IServiceScopeFactory scopeFactory, ILogger<CommandConsumer<TCommand, TCommandHandler>> logger)
        : base(configuration, consumerConfig, producer, scopeFactory, logger) { }

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
