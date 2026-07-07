using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;

/// <summary>
/// The consumer hosting one event subscriber: binds its queue to the event's <c>fanout</c> exchange
/// (broadcast — one queue per subscriber, each gets a copy) and, per delivery, deserializes the event
/// once and invokes the subscriber resolved from the delivery's scope. A subscriber failure goes to
/// the event's error handler, a malformed delivery (or a relayed failure) to its fault handler — both
/// resolved from the scope.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventWorker<TEvent, TEventSubscriber> : ConsumerWorker<EventContext<TEvent>, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    /// <summary>Creates the event consumer over the shared connection, the scope factory, the logger and its topology.</summary>
    /// <param name="connection">The shared RabbitMQ connection the worker's channel is opened on.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The event's exchange.</param>
    /// <param name="queue">The queue this subscriber consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    public EventWorker(Domain.IConnection connection, IServiceScopeFactory scopeFactory, ILogger<EventWorker<TEvent, TEventSubscriber>> logger, string exchange, string queue, ushort prefetchCount)
        : base(connection, scopeFactory, logger, exchange, ExchangeType.Fanout, queue, prefetchCount)
    {
    }

    /// <inheritdoc />
    protected override EventContext<TEvent> CreateContext(BasicDeliverEventArgs args)
        => new(JsonSerializer.Deserialize<TEvent>(args.Body.Span) ?? throw new JsonException("The event body deserialized to null."), Transport.Create(args));

    /// <inheritdoc />
    protected override Task Handle(TEventSubscriber handler, EventContext<TEvent> context, CancellationToken cancellationToken)
        => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorResult> HandleError(IServiceProvider services, EventContext<TEvent> context, Exception exception, CancellationToken cancellationToken)
    {
        Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber> handler = services.GetRequiredService<Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>>();

        await handler.Handle(new EventErrorContext<TEvent>(context.Message, context.Transport, exception), cancellationToken);

        return handler.Result;
    }

    /// <inheritdoc />
    protected override async Task<FaultResult> HandleFault(IServiceProvider services, ReadOnlyMemory<byte> body, Transport transport, Exception exception, CancellationToken cancellationToken)
    {
        Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber> handler = services.GetRequiredService<Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber>>();

        await handler.Handle(EventFaultContext.Create(body, transport, exception), cancellationToken);

        return handler.Result;
    }
}
