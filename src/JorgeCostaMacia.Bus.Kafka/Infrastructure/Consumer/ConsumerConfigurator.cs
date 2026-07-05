using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumer;

/// <summary>
/// The consume side of the bus's configuration, self-contained: it binds its own
/// <see cref="ConsumerConfiguration"/> from the <c>Bus:Consumer</c> section, and registers the
/// service's handlers (<see cref="AddCommandHandler{TCommand, TCommandHandler}"/> /
/// <see cref="AddEventSubscriber{TEvent, TEventSubscriber}"/>) — each with its hosted consumer and
/// the framework's error and fault handlers wired in. It reads (never writes) the routing map the
/// <see cref="Producer.ProducerConfigurator"/> owns to resolve each handler's topic by type. The error
/// handling is the framework's; the service tunes only the resilience policy here.
/// </summary>
public sealed class ConsumerConfigurator
{
    private const string CONSUMER_SECTION = "Bus:Consumer";

    private readonly IServiceCollection _services;
    private readonly IReadOnlyDictionary<Type, string> _messages;
    private readonly ConsumerConfiguration? _configuration;

    /// <summary>Binds the consumer configuration from the <c>Bus:Consumer</c> section and takes the routing map to resolve topics.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="messages">The type → topic routing map the producer side owns.</param>
    /// <exception cref="InvalidOperationException">The <c>Bus:Consumer</c> section is present but one of its required values is missing.</exception>
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
        where TCommandHandler : class, IHandler<TCommand, CommandContext<TCommand>>
    {
        string topic = Topic(typeof(TCommand));
        ConsumerConfig configuration = ConsumerConfig(groupId);

        _services.AddScoped<TCommandHandler>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<Commands.CommandWorker<TCommand, TCommandHandler>> logger = provider.GetRequiredService<ILogger<Commands.CommandWorker<TCommand, TCommandHandler>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();

            return new Commands.CommandWorker<TCommand, TCommandHandler>(
                CreateBuilder(provider, configuration, logger, lifetime),
                new Commands.CommandErrorHandler<TCommand>(Bus(provider), provider.GetService<IRetryScheduler>(), logger, topic, groupId, Intervals(retryIntervals), Excludes(retryExcludeExceptionTypes)),
                CreateFaultHandler(provider, logger, topic, groupId),
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
        where TEventSubscriber : class, IHandler<TEvent, EventContext<TEvent>>
    {
        string topic = Topic(typeof(TEvent));
        ConsumerConfig configuration = ConsumerConfig(groupId);

        _services.AddScoped<TEventSubscriber>();
        _services.AddSingleton<IHostedService>(provider =>
        {
            ILogger<Events.EventWorker<TEvent, TEventSubscriber>> logger = provider.GetRequiredService<ILogger<Events.EventWorker<TEvent, TEventSubscriber>>>();
            IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();

            return new Events.EventWorker<TEvent, TEventSubscriber>(
                CreateBuilder(provider, configuration, logger, lifetime),
                new Events.EventErrorHandler<TEvent>(Bus(provider), provider.GetService<IRetryScheduler>(), logger, topic, groupId, Intervals(retryIntervals), Excludes(retryExcludeExceptionTypes)),
                CreateFaultHandler(provider, logger, topic, groupId),
                provider.GetRequiredService<IServiceScopeFactory>(),
                logger,
                lifetime,
                topic,
                groupId);
        });

        return this;
    }

    /// <summary>The topic the message was mapped to on the producer side, or a throw when it was not mapped first.</summary>
    private string Topic(Type message)
        => _messages.TryGetValue(message, out string? topic)
            ? topic
            : throw new InvalidOperationException($"'{message.Name}' is not mapped to a topic; map it with AddCommand/AddEvent first.");

    /// <summary>The consumer's Kafka settings for the group, or a throw when the consumer section is absent.</summary>
    private ConsumerConfig ConsumerConfig(string groupId)
        => _configuration?.ConsumerConfig(groupId)
            ?? throw new InvalidOperationException($"'{CONSUMER_SECTION}' is null.");

    /// <summary>
    /// Binds the <c>Bus:Consumer</c> section onto a <see cref="ConsumerConfiguration"/>, or
    /// <see langword="null"/> when the section is absent — a producer-only service maps no handlers and
    /// needs no section; a mapped handler then throws.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global consumer configuration, or <see langword="null"/> when the section is absent.</returns>
    /// <exception cref="InvalidOperationException">The section is present but one of its required values is missing.</exception>
    private static ConsumerConfiguration? CreateConsumerConfiguration(IConfiguration configuration)
    {
        ConsumerConfiguration? consumerConfiguration = configuration.GetSection(CONSUMER_SECTION).Get<ConsumerConfiguration>();

        if (consumerConfiguration is null)
        {
            return null;
        }

        Validate(nameof(consumerConfiguration.BootstrapServers), consumerConfiguration.BootstrapServers);
        Validate(nameof(consumerConfiguration.SaslUsername), consumerConfiguration.SaslUsername);
        Validate(nameof(consumerConfiguration.SaslPassword), consumerConfiguration.SaslPassword);

        return consumerConfiguration;
    }

    /// <summary>Throws when a required configuration value is missing — the binder does not enforce <see langword="required"/> members.</summary>
    /// <param name="name">The value's name within the section.</param>
    /// <param name="value">The bound value.</param>
    /// <exception cref="InvalidOperationException">The value is missing.</exception>
    private static void Validate(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"'{CONSUMER_SECTION}:{name}' is null.");
        }
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

    /// <summary>Composes a consumer's fault handler over the bus, the logger and its contract.</summary>
    private static Faults.FaultHandler CreateFaultHandler(IServiceProvider provider, ILogger logger, string topic, string groupId)
        => new(Bus(provider), logger, topic, groupId);

    /// <summary>The bus — the single outbound gate the error and fault handlers produce through.</summary>
    private static Bus Bus(IServiceProvider provider)
        => provider.GetRequiredService<Bus>();

    /// <summary>The retry ladder, or the default when none is supplied.</summary>
    private static ImmutableList<TimeSpan> Intervals(ImmutableList<TimeSpan>? retryIntervals)
        => retryIntervals ?? ConsumerWorkerDefaults.RETRY_INTERVALS;

    /// <summary>The retry-excluded exception types, or the default when none is supplied.</summary>
    private static ImmutableList<Type> Excludes(ImmutableList<Type>? retryExcludeExceptionTypes)
        => retryExcludeExceptionTypes ?? ConsumerWorkerDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;
}
