using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Events;

/// <summary>
/// The consumer hosting one event subscriber. Events are pub/sub (one group per subscriber), so it
/// overrides consumer-side filtering: deliveries targeting other consumers are skipped and acked,
/// straight off the raw header. On a failure it hands the delivery to its event error handler (the
/// framework mechanics — retry ladder targeted to this group, error parking) over the context
/// already built, so the event deserialized once serves the subscriber and the error handler alike.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventWorker<TEvent, TEventSubscriber> : ConsumerWorker<EventContext<TEvent>, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    /// <summary>Creates the consumer over its inbound gate, the scope factory, the logger and its contract — the subscriber and its error and fault handlers are resolved per delivery from the scope.</summary>
    /// <param name="consumer">The inbound gate over the Kafka client — its settings and logging handlers already wired.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="lifetime">The application lifetime — stopped when the client reports an unrecoverable state.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to.</param>
    /// <param name="groupId">The consumer group id — the consumer's identity for offsets and consumer-side filtering.</param>
    public EventWorker(
        IConsumer consumer,
        IServiceScopeFactory scopeFactory,
        ILogger<EventWorker<TEvent, TEventSubscriber>> logger,
        IHostApplicationLifetime lifetime,
        string topic,
        string groupId)
        : base(consumer, scopeFactory, logger, lifetime, topic, groupId)
    {
    }

    /// <inheritdoc />
    protected override EventContext<TEvent> CreateContext(ConsumeResult<Ignore, byte[]> result, Transport transport)
        => new(JsonSerializer.Deserialize<TEvent>(result.Message.Value, BusSerializer.Options) ?? throw new JsonException("The event body deserialized to null."), transport);

    /// <inheritdoc />
    protected override Task Handle(TEventSubscriber handler, EventContext<TEvent> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorResult> HandleError(IServiceProvider services, EventContext<TEvent> context, Exception exception, CancellationToken cancellationToken)
    {
        Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber> errorHandler = services.GetRequiredService<Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>>();

        await errorHandler.Handle(new EventErrorContext<TEvent>(context.Message, context.Transport, exception), cancellationToken);

        return errorHandler.Result;
    }

    /// <inheritdoc />
    protected override async Task<FaultResult> HandleFault(IServiceProvider services, byte[] body, Transport transport, Exception exception, CancellationToken cancellationToken)
    {
        Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber> faultHandler = services.GetRequiredService<Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber>>();

        await faultHandler.Handle(EventFaultContext.Create(body, transport, exception), cancellationToken);

        return faultHandler.Result;
    }

    /// <summary>
    /// Consumer-side filtering: when the message targets specific consumers
    /// (<c>AggregateConsumers</c> header non-empty) and this group is not among them, the delivery is
    /// skipped (and acked) without deserializing the body.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected override bool Filtered(ConsumeResult<Ignore, byte[]> result)
    {
        if (!result.Message.Headers.TryGetLastBytes(TransportHeaders.AggregateConsumers, out byte[] header)) return false;

        string consumers = Encoding.UTF8.GetString(header);

        if (string.IsNullOrWhiteSpace(consumers)) return false;

        return !consumers
            .Split(',')
            .Select(consumer => consumer.Trim())
            .Contains(GroupId);
    }
}
