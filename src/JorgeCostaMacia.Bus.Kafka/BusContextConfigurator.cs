using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
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
    private readonly ConsumerConfiguration? _configuration;
    private readonly Dictionary<Type, string> _messages;

    internal BusContextConfigurator(IServiceCollection services, ConsumerConfiguration? configuration, Dictionary<Type, string> messages)
    {
        _services = services;
        _configuration = configuration;
        _messages = messages;
    }

    /// <summary>Registers a command this service sends, mapping its type to a topic.</summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="topic">The Kafka topic the command is sent to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddCommand<TCommand>(string topic)
        where TCommand : Command
    {
        _messages.Add(typeof(TCommand), topic);

        return this;
    }

    /// <summary>Registers an event this service publishes, mapping its type to a topic.</summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="topic">The Kafka topic the event is published to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusContextConfigurator AddEvent<TEvent>(string topic)
        where TEvent : Event
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
        where TCommand : Command
        where TCommandHandler : class, IHandler<TCommand, CommandContext<TCommand>>
    {
        ConsumerConfig configuration = _configuration?.ConsumerConfig(groupId)
            ?? throw new InvalidOperationException($"'{BusInfrastructureContext.CONSUMER_SECTION}' is null.");

        _services.AddScoped<TCommandHandler>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<CommandConsumerWorker<TCommand, TCommandHandler>> logger = provider.GetRequiredService<ILogger<CommandConsumerWorker<TCommand, TCommandHandler>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();

            return new CommandConsumerWorker<TCommand, TCommandHandler>(
                CreateBuilder(provider, configuration, logger, lifetime),
                CreateErrorHandler(provider, logger, topic, groupId, retryIntervals, retryExcludeExceptionTypes),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                lifetime,
                topic,
                groupId);
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
        where TEvent : Event
        where TEventSubscriber : class, IHandler<TEvent, EventContext<TEvent>>
    {
        ConsumerConfig configuration = _configuration?.ConsumerConfig(groupId)
            ?? throw new InvalidOperationException($"'{BusInfrastructureContext.CONSUMER_SECTION}' is null.");

        _services.AddScoped<TEventSubscriber>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<EventConsumerWorker<TEvent, TEventSubscriber>> logger = provider.GetRequiredService<ILogger<EventConsumerWorker<TEvent, TEventSubscriber>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();

            return new EventConsumerWorker<TEvent, TEventSubscriber>(
                CreateBuilder(provider, configuration, logger, lifetime),
                CreateErrorHandler(provider, logger, topic, groupId, retryIntervals, retryExcludeExceptionTypes),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                lifetime,
                topic,
                groupId);
        });

        return this;
    }

    /// <summary>
    /// Composes a consumer's Kafka builder: the settings plus every callback wired — the client's
    /// error/log/statistics to the Kafka category (a fatal error stops the application), the commit
    /// results and the partition lifecycle to the worker's logger.
    /// </summary>
    private static ConsumerBuilder<Ignore, byte[]> CreateBuilder(IServiceProvider provider, ConsumerConfig configuration, ILogger logger, IHostApplicationLifetime lifetime)
    {
        ILogger kafkaLogger = provider.GetRequiredService<ILoggerFactory>().CreateLogger(KafkaLogger.Category);

        return new ConsumerBuilder<Ignore, byte[]>(configuration)
            .SetErrorHandler((_, kafkaError) =>
            {
                KafkaLogger.LogError(kafkaLogger, kafkaError);

                if (kafkaError.IsFatal) lifetime.StopApplication();
            })
            .SetLogHandler((_, log) => KafkaLogger.Log(kafkaLogger, log))
            .SetStatisticsHandler((_, statistics) => KafkaLogger.LogStatistics(kafkaLogger, statistics))
            .SetOffsetsCommittedHandler((_, committed) => BusLogger.LogCommit(logger, committed))
            .SetPartitionsAssignedHandler((_, partitions) => BusLogger.LogPartitionsAssigned(logger, partitions))
            .SetPartitionsRevokedHandler((_, partitions) => BusLogger.LogPartitionsRevoked(logger, partitions))
            .SetPartitionsLostHandler((_, partitions) => BusLogger.LogPartitionsLost(logger, partitions));
    }

    /// <summary>Composes a consumer's failure policy, the defaults applied.</summary>
    private static ConsumerErrorHandler CreateErrorHandler(IServiceProvider provider, ILogger logger, string topic, string groupId, ImmutableList<TimeSpan>? retryIntervals, ImmutableList<Type>? retryExcludeExceptionTypes)
        => new(
            provider.GetRequiredService<Infrastructure.Bus>(),
            provider.GetService<IRetryScheduler>(),
            logger,
            topic,
            groupId,
            retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS,
            retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES);
}
