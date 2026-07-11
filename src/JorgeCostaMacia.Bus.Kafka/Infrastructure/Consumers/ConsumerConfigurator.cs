using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

/// <summary>
/// The consume side of the bus's configuration, self-contained: it binds its own
/// <see cref="ConsumerConfiguration"/> from the <c>Bus:Consumer</c> section, and registers the
/// service's handlers (<see cref="AddCommandHandler{TCommand, TCommandHandler}"/> /
/// <see cref="AddEventSubscriber{TEvent, TEventSubscriber}"/>) — each with its hosted consumer and
/// the framework's error and fault handlers wired in. It reads (never writes) the routing map the
/// <see cref="Producers.ProducerConfigurator"/> owns to resolve each handler's topic by type. The error
/// handling is the framework's; the service tunes only the resilience policy here.
/// </summary>
public sealed class ConsumerConfigurator
{
    private const string CONSUMER_SECTION = "Bus:Consumer";

    private readonly IServiceCollection _services;
    private readonly IReadOnlyDictionary<Type, string> _messages;
    private readonly ConsumerConfiguration _configuration;
    private readonly HashSet<string> _groupIds = new(StringComparer.Ordinal);

    /// <summary>Binds the consumer configuration from the <c>Bus:Consumer</c> section and takes the routing map to resolve topics.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="messages">The type → topic routing map the producer side owns.</param>
    /// <exception cref="InvalidOperationException">The <c>Bus:Consumer</c> section or one of its required values is missing.</exception>
    internal ConsumerConfigurator(IServiceCollection services, IConfiguration configuration, IReadOnlyDictionary<Type, string> messages)
    {
        _services = services;
        _messages = messages;
        _configuration = CreateConsumerConfiguration(configuration);
    }

    /// <summary>
    /// Registers a command handler: the handler itself (scoped, one per delivery) and its hosted
    /// consumer, with the framework's error and fault handlers wired in. The topic is the one the
    /// command was mapped to on the producer side (map it first). Its Kafka settings come from the
    /// <c>Bus:Consumer</c> section; the service tunes only the resilience policy here.
    /// </summary>
    /// <typeparam name="TCommand">The command type consumed.</typeparam>
    /// <typeparam name="TCommandHandler">The handler type.</typeparam>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>) — a stable contract, it holds the group's offsets.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> requeues immediately), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ConsumerConfigurator AddCommandHandler<TCommand, TCommandHandler>(
        string groupId,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null)
        where TCommand : Command
        where TCommandHandler : CommandHandler<TCommand>
    {
        RegisterGroupId(groupId);

        string topic = _messages.TryGetValue(typeof(TCommand), out string? mapped)
            ? mapped
            : throw new InvalidOperationException($"'{typeof(TCommand).FullName}' is not mapped to a topic; map it with AddCommand/AddEvent first.");
        ConsumerConfig configuration = _configuration.ConsumerConfig(groupId);
        ImmutableList<TimeSpan> intervals = retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS;
        ImmutableList<Type> excludes = retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;

        _services.AddScoped<TCommandHandler>();

        _services.AddScoped<Domain.Commands.Errors.CommandErrorHandler<TCommand, TCommandHandler>>(provider =>
            new Commands.CommandErrorHandler<TCommand, TCommandHandler>(
                provider.GetRequiredService<IProducer>(),
                provider.GetService<IRetryScheduler>(),
                provider.GetRequiredService<ILogger<Commands.CommandErrorHandler<TCommand, TCommandHandler>>>(),
                topic,
                groupId,
                intervals,
                excludes));

        _services.AddScoped<Domain.Commands.Faults.CommandFaultHandler<TCommand, TCommandHandler>>(provider =>
            new Commands.CommandFaultHandler<TCommand, TCommandHandler>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Commands.CommandFaultHandler<TCommand, TCommandHandler>>>(),
                topic,
                groupId));

        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<Commands.CommandWorker<TCommand, TCommandHandler>> logger = provider.GetRequiredService<ILogger<Commands.CommandWorker<TCommand, TCommandHandler>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();
            BusHealth health = provider.GetRequiredService<BusHealth>();

            return new Commands.CommandWorker<TCommand, TCommandHandler>(
                new Consumer(CreateBuilder(provider, configuration, logger, lifetime, health)),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                lifetime,
                health,
                topic,
                groupId);
        });

        return this;
    }

    /// <summary>
    /// Registers an event subscriber: the subscriber itself (scoped, one per delivery) and its hosted
    /// consumer, with the framework's error and fault handlers wired in. The topic is the one the
    /// event was mapped to on the producer side (map it first). Its Kafka settings come from the
    /// <c>Bus:Consumer</c> section; the service tunes only the resilience policy here.
    /// </summary>
    /// <typeparam name="TEvent">The event type consumed.</typeparam>
    /// <typeparam name="TEventSubscriber">The subscriber type.</typeparam>
    /// <param name="groupId">The consumer group id (e.g. <c>{consumer}.on.{topic}.subscriber</c>) — a stable contract, unique per subscriber, it holds the group's offsets.</param>
    /// <param name="retryIntervals">Delays before each retry when handling fails (one entry per attempt, <c>00:00</c> requeues immediately), or <see langword="null"/> for the default (none).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retry, or <see langword="null"/> for none.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ConsumerConfigurator AddEventSubscriber<TEvent, TEventSubscriber>(
        string groupId,
        ImmutableList<TimeSpan>? retryIntervals = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null)
        where TEvent : Event
        where TEventSubscriber : EventSubscriber<TEvent>
    {
        RegisterGroupId(groupId);

        string topic = _messages.TryGetValue(typeof(TEvent), out string? mapped)
            ? mapped
            : throw new InvalidOperationException($"'{typeof(TEvent).FullName}' is not mapped to a topic; map it with AddCommand/AddEvent first.");
        ConsumerConfig configuration = _configuration.ConsumerConfig(groupId);
        ImmutableList<TimeSpan> intervals = retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS;
        ImmutableList<Type> excludes = retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;

        _services.AddScoped<TEventSubscriber>();

        _services.AddScoped<Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>>(provider =>
            new Events.EventErrorHandler<TEvent, TEventSubscriber>(
                provider.GetRequiredService<IProducer>(),
                provider.GetService<IRetryScheduler>(),
                provider.GetRequiredService<ILogger<Events.EventErrorHandler<TEvent, TEventSubscriber>>>(),
                topic,
                groupId,
                intervals,
                excludes));

        _services.AddScoped<Domain.Events.Faults.EventFaultHandler<TEvent, TEventSubscriber>>(provider =>
            new Events.EventFaultHandler<TEvent, TEventSubscriber>(
                provider.GetRequiredService<IProducer>(),
                provider.GetRequiredService<ILogger<Events.EventFaultHandler<TEvent, TEventSubscriber>>>(),
                topic,
                groupId));

        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<Events.EventWorker<TEvent, TEventSubscriber>> logger = provider.GetRequiredService<ILogger<Events.EventWorker<TEvent, TEventSubscriber>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();
            BusHealth health = provider.GetRequiredService<BusHealth>();

            return new Events.EventWorker<TEvent, TEventSubscriber>(
                new Consumer(CreateBuilder(provider, configuration, logger, lifetime, health)),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                lifetime,
                health,
                topic,
                groupId);
        });

        return this;
    }

    /// <summary>
    /// Tracks every registered consumer group id and rejects a duplicate — like the message → topic
    /// map, the registry is the single source the registrations check against. Two consumers sharing
    /// a group id would also share the default machine-name <c>group.instance.id</c> and fence each
    /// other out of the group (a fatal broker error that stops the application at startup).
    /// </summary>
    /// <param name="groupId">The consumer group id being registered.</param>
    /// <exception cref="InvalidOperationException">The group id is already registered by another handler.</exception>
    private void RegisterGroupId(string groupId)
    {
        if (!_groupIds.Add(groupId)) throw new InvalidOperationException($"Consumer group id '{groupId}' is already registered; give each handler its own group id.");
    }

    /// <summary>
    /// Binds the <c>Bus:Consumer</c> section onto a <see cref="ConsumerConfiguration"/> — mandatory,
    /// since the configurator is only built when the app opts into consuming (a send-only service
    /// omits the consumer lambda and never reaches here).
    /// </summary>
    /// <returns>The global consumer configuration.</returns>
    /// <exception cref="InvalidOperationException">The section or one of its required values is missing.</exception>
    private static ConsumerConfiguration CreateConsumerConfiguration(IConfiguration configuration)
    {
        ConsumerConfiguration consumerConfiguration = configuration.GetSection(CONSUMER_SECTION).Get<ConsumerConfiguration>()
            ?? throw new InvalidOperationException($"'{CONSUMER_SECTION}' is null.");

        if (string.IsNullOrWhiteSpace(consumerConfiguration.BootstrapServers)) throw new InvalidOperationException($"'{CONSUMER_SECTION}:{nameof(consumerConfiguration.BootstrapServers)}' is null.");
        if (string.IsNullOrWhiteSpace(consumerConfiguration.SaslUsername)) throw new InvalidOperationException($"'{CONSUMER_SECTION}:{nameof(consumerConfiguration.SaslUsername)}' is null.");
        if (string.IsNullOrWhiteSpace(consumerConfiguration.SaslPassword)) throw new InvalidOperationException($"'{CONSUMER_SECTION}:{nameof(consumerConfiguration.SaslPassword)}' is null.");

        return consumerConfiguration;
    }

    /// <summary>
    /// Composes a consumer's Kafka builder: the settings plus every callback wired — the client's
    /// error/log/statistics to the Kafka category (a fatal error stops the application, an
    /// <c>AllBrokersDown</c> flips the reachability tracker down), the commit results and the
    /// partition lifecycle to the worker's logger.
    /// </summary>
    private static ConsumerBuilder<Ignore, byte[]> CreateBuilder(IServiceProvider provider, ConsumerConfig configuration, ILogger logger, IHostApplicationLifetime lifetime, BusHealth health)
    {
        ILogger kafkaLogger = provider.GetRequiredService<ILoggerFactory>().CreateLogger(KafkaLogger.Category);

        return new ConsumerBuilder<Ignore, byte[]>(configuration)
            .SetErrorHandler((_, kafkaError) =>
            {
                KafkaLogger.LogError(kafkaLogger, kafkaError);

                if (kafkaError.Code == ErrorCode.Local_AllBrokersDown) health.Down();
                if (kafkaError.IsFatal) lifetime.StopApplication();
            })
            .SetLogHandler((_, log) => KafkaLogger.Log(kafkaLogger, log))
            .SetStatisticsHandler((_, statistics) => KafkaLogger.LogStatistics(kafkaLogger, statistics))
            .SetOffsetsCommittedHandler((_, committed) => BusLogger.LogCommit(logger, committed))
            .SetPartitionsAssignedHandler((_, partitions) => BusLogger.LogPartitionsAssigned(logger, partitions))
            .SetPartitionsRevokedHandler((_, partitions) => BusLogger.LogPartitionsRevoked(logger, partitions))
            .SetPartitionsLostHandler((_, partitions) => BusLogger.LogPartitionsLost(logger, partitions));
    }
}
