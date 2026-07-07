using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// The consume side of the bus's configuration: registers the service's handlers
/// (<see cref="AddCommandHandler{TCommand, TCommandHandler}"/> /
/// <see cref="AddEventSubscriber{TEvent, TEventSubscriber}"/>) — each with its own hosted consumer and
/// the framework's error and fault handlers wired in. The consumer binds its queue to the message's
/// exchange (a command's <c>direct</c>, an event's <c>fanout</c>) and declares its <c>.error</c> /
/// <c>.fault</c> park queues. It reads (never writes) the routing map the
/// <see cref="Producers.ProducerConfigurator"/> owns to resolve each handler's exchange by type. The
/// shared connection is bound once elsewhere.
/// </summary>
public sealed class ConsumerConfigurator
{
    private const ushort DEFAULT_PREFETCH_COUNT = 10;

    private readonly IServiceCollection _services;
    private readonly IReadOnlyDictionary<Type, string> _messages;

    /// <summary>Takes the service collection and the routing map to resolve exchanges.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="messages">The type → exchange routing map the producer side owns.</param>
    internal ConsumerConfigurator(IServiceCollection services, IReadOnlyDictionary<Type, string> messages)
    {
        _services = services;
        _messages = messages;
    }

    /// <summary>Registers a command handler (scoped, one per delivery), its error and fault handlers, and its hosted consumer on the queue.</summary>
    /// <typeparam name="TCommand">The command type consumed.</typeparam>
    /// <typeparam name="TCommandHandler">The handler type.</typeparam>
    /// <param name="queue">The queue this handler consumes, bound to the command's exchange.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> re-publishes to the exchange immediately; a positive delay parks, as RabbitMQ has no scheduler yet), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <param name="prefetchCount">The maximum unacked messages delivered before waiting for acks.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ConsumerConfigurator AddCommandHandler<TCommand, TCommandHandler>(
        string queue,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        ushort prefetchCount = DEFAULT_PREFETCH_COUNT)
        where TCommand : Command
        where TCommandHandler : CommandHandler<TCommand>
    {
        string exchange = Exchange<TCommand>();
        ImmutableList<TimeSpan> intervals = retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS;
        ImmutableList<Type> excludes = retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;

        _services.AddScoped<TCommandHandler>();

        _services.AddScoped<Domain.Commands.Errors.CommandErrorHandler<TCommand, TCommandHandler>>(provider =>
            new Commands.CommandErrorHandler<TCommand, TCommandHandler>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Commands.CommandErrorHandler<TCommand, TCommandHandler>>>(),
                exchange,
                queue,
                intervals,
                excludes));

        _services.AddScoped<Domain.Commands.Faults.CommandFaultHandler<TCommand, TCommandHandler>>(provider =>
            new Commands.CommandFaultHandler<TCommand, TCommandHandler>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Commands.CommandFaultHandler<TCommand, TCommandHandler>>>(),
                queue));

        _services.AddSingleton<IHostedService>(provider => new Commands.CommandWorker<TCommand, TCommandHandler>(
            provider.GetRequiredService<Domain.IConnection>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<Commands.CommandWorker<TCommand, TCommandHandler>>>(),
            exchange,
            queue,
            prefetchCount));

        return this;
    }

    /// <summary>Registers an event subscriber (scoped, one per delivery), its error and fault handlers, and its hosted consumer on the queue.</summary>
    /// <typeparam name="TEvent">The event type consumed.</typeparam>
    /// <typeparam name="TEventSubscriber">The subscriber type.</typeparam>
    /// <param name="queue">The queue this subscriber consumes, bound to the event's exchange.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> re-publishes to the exchange immediately; a positive delay parks, as RabbitMQ has no scheduler yet), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <param name="prefetchCount">The maximum unacked messages delivered before waiting for acks.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ConsumerConfigurator AddEventSubscriber<TEvent, TEventSubscriber>(
        string queue,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        ushort prefetchCount = DEFAULT_PREFETCH_COUNT)
        where TEvent : Event
        where TEventSubscriber : EventSubscriber<TEvent>
    {
        string exchange = Exchange<TEvent>();
        ImmutableList<TimeSpan> intervals = retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS;
        ImmutableList<Type> excludes = retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;

        _services.AddScoped<TEventSubscriber>();

        _services.AddScoped<Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>>(provider =>
            new Events.EventErrorHandler<TEvent, TEventSubscriber>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Events.EventErrorHandler<TEvent, TEventSubscriber>>>(),
                exchange,
                queue,
                intervals,
                excludes));

        _services.AddScoped<Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber>>(provider =>
            new Events.EventFaultHandler<TEvent, TEventSubscriber>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Events.EventFaultHandler<TEvent, TEventSubscriber>>>(),
                queue));

        _services.AddSingleton<IHostedService>(provider => new Events.EventWorker<TEvent, TEventSubscriber>(
            provider.GetRequiredService<Domain.IConnection>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<Events.EventWorker<TEvent, TEventSubscriber>>>(),
            exchange,
            queue,
            prefetchCount));

        return this;
    }

    /// <summary>Resolves the exchange a message type was mapped to on the producer side.</summary>
    private string Exchange<TMessage>()
        => _messages.TryGetValue(typeof(TMessage), out string? exchange)
            ? exchange
            : throw new InvalidOperationException($"'{typeof(TMessage).FullName}' is not mapped to an exchange; map it with AddCommand/AddEvent first.");
}
