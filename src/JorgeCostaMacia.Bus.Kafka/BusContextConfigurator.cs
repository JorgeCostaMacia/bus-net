using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka;

/// <summary>
/// Registers the bus's messages and handlers. The Kafka consumer settings come from the
/// <c>Bus:Consumer</c> configuration section (declared once, never repeated); each handler declares
/// only its own contract — topic, group id and resilience policy. Each context maps its own pieces
/// through it (<c>Map*BusContext(this BusContextConfigurator)</c> extensions): messages it sends
/// (<see cref="AddCommand{TCommand}"/> / <see cref="AddEvent{TEvent}"/>) and messages it consumes
/// (<see cref="AddCommandHandler{TCommand, TCommandHandler}"/> /
/// <see cref="AddEventSubscriber{TEvent, TEventSubscriber}"/> — each hosting its own consumer).
/// </summary>
public sealed class BusContextConfigurator
{
    private readonly IServiceCollection _services;
    private readonly KafkaConsumerConfiguration _consumer;
    private readonly Dictionary<Type, string> _messages = [];

    /// <summary>The type → topic routing map accumulated by the contexts.</summary>
    internal IReadOnlyDictionary<Type, string> Messages => _messages;

    internal BusContextConfigurator(IServiceCollection services, KafkaConsumerConfiguration consumer)
    {
        _services = services;
        _consumer = consumer;
    }

    /// <summary>Registers a command this service sends, mapping its type to a topic.</summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="topic">The Kafka topic the command is sent to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddCommand<TCommand>(string topic)
        where TCommand : ICommand
    {
        _messages.Add(typeof(TCommand), topic);

        return this;
    }

    /// <summary>Registers an event this service publishes, mapping its type to a topic.</summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="topic">The Kafka topic the event is published to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddEvent<TEvent>(string topic)
        where TEvent : IEvent
    {
        _messages.Add(typeof(TEvent), topic);

        return this;
    }

    /// <summary>
    /// Registers a command handler: the handler itself (scoped, one per delivery) and its hosted
    /// consumer — its custom policy here, its Kafka settings from the global configuration.
    /// </summary>
    /// <typeparam name="TCommand">The command type consumed.</typeparam>
    /// <typeparam name="TCommandHandler">The handler type.</typeparam>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>) — a stable contract, it holds the group's offsets.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> requeues immediately), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddCommandHandler<TCommand, TCommandHandler>(
        string topic,
        string groupId,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null)
        where TCommand : Domain.Command
        where TCommandHandler : class, ICommandHandler<TCommand, CommandContext<TCommand>, Transport>
    {
        ConsumerConfig consumer = _consumer.ConsumerConfig(groupId);

        _services.AddScoped<TCommandHandler>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<CommandConsumerWorker<TCommand, TCommandHandler>> logger = provider.GetRequiredService<ILogger<CommandConsumerWorker<TCommand, TCommandHandler>>>();

            ConsumerBuilder<Null, byte[]> builder = new ConsumerBuilder<Null, byte[]>(consumer)
                .SetErrorHandler((_, error) => KafkaConsumerLogger.LogError(logger, error))
                .SetLogHandler((_, log) => KafkaConsumerLogger.Log(logger, log));

            return new CommandConsumerWorker<TCommand, TCommandHandler>(
                builder,
                provider.GetRequiredService<IProducer<Null, byte[]>>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                topic,
                groupId,
                retryIntervals ?? ConsumerConfigurationDefaults.RETRY_INTERVALS,
                retryExcludeExceptionTypes ?? ConsumerConfigurationDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES);
        });

        return this;
    }

    /// <summary>
    /// Registers an event subscriber: the subscriber itself (scoped, one per delivery) and its hosted
    /// consumer — its custom policy here, its Kafka settings from the global configuration.
    /// </summary>
    /// <typeparam name="TEvent">The event type consumed.</typeparam>
    /// <typeparam name="TEventSubscriber">The subscriber type.</typeparam>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{consumer}.on.{topic}.subscriber</c>) — a stable contract, unique per subscriber, it holds the group's offsets.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> requeues immediately), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddEventSubscriber<TEvent, TEventSubscriber>(
        string topic,
        string groupId,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null)
        where TEvent : Domain.Event
        where TEventSubscriber : class, IEventSubscriber<TEvent, EventContext<TEvent>, Transport>
    {
        ConsumerConfig consumer = _consumer.ConsumerConfig(groupId);

        _services.AddScoped<TEventSubscriber>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<EventConsumerWorker<TEvent, TEventSubscriber>> logger = provider.GetRequiredService<ILogger<EventConsumerWorker<TEvent, TEventSubscriber>>>();

            ConsumerBuilder<Null, byte[]> builder = new ConsumerBuilder<Null, byte[]>(consumer)
                .SetErrorHandler((_, error) => KafkaConsumerLogger.LogError(logger, error))
                .SetLogHandler((_, log) => KafkaConsumerLogger.Log(logger, log));

            return new EventConsumerWorker<TEvent, TEventSubscriber>(
                builder,
                provider.GetRequiredService<IProducer<Null, byte[]>>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                topic,
                groupId,
                retryIntervals ?? ConsumerConfigurationDefaults.RETRY_INTERVALS,
                retryExcludeExceptionTypes ?? ConsumerConfigurationDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES);
        });

        return this;
    }
}
