using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The package's wiring: it lets the send side map its messages through the
/// <see cref="ProducerConfigurator"/> (which binds <c>Bus:Producer</c> and owns the routing map) and
/// the consume side register its handlers through the <see cref="ConsumerConfigurator"/> (which binds
/// <c>Bus:Consumer</c> and reads that map). The shared Kafka producer (error/log callbacks wired to
/// its logger) is a container-owned singleton, wrapped by the <see cref="IProducer"/> gate every
/// outbound byte goes through; the <see cref="Bus"/> — registered only behind <see cref="IBus"/> —
/// and the consumers' handlers both produce through that gate, while the <c>ProducerWorker</c> owns
/// the raw producer's lifecycle. The worker is registered before the consumers so it stops last: the
/// consumers stop before its final flush. The bus itself is a lazy singleton, built last.
/// </summary>
internal static class BusInfrastructureContext
{
    /// <summary>
    /// Registers the bus over the application configuration: the producer lambda maps the messages to
    /// topics (its routing map feeds both the bus and the consumers); the optional consumer lambda
    /// registers the handlers — omit it for a send-only service, which then needs no <c>Bus:Consumer</c>
    /// section.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> section (and <c>Bus:Consumer</c> when a consumer lambda is supplied).</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their topics.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers, or <see langword="null"/> for a send-only service.</param>
    /// <param name="admin">Declares the topics to create at startup (over the dedicated <c>Bus:Admin</c> connection), or <see langword="null"/> to leave provisioning to the broker.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration, Action<ProducerConfigurator> producer, Action<ConsumerConfigurator>? consumer = null, Action<AdminConfigurator>? admin = null)
    {
        ProducerConfigurator producerConfigurator = new ProducerConfigurator(configuration);

        producer(producerConfigurator);

        // opt-in topic provisioning: declared through the admin configurator (a dedicated Bus:Admin
        // connection) and created before every other hosted service, so the topics exist by the time the
        // consumers subscribe (no "unknown topic" churn). Omit the admin configurator to leave provisioning
        // to the broker. The error/fault topics are never created here — they are born on the first parked
        // message, so their presence signals a real failure.
        if (admin is not null)
        {
            AdminConfigurator adminConfigurator = new AdminConfigurator(configuration);

            admin(adminConfigurator);

            AdminClientConfig adminClientConfig = adminConfigurator.AdminClientConfig;
            IReadOnlyDictionary<string, int> topicSpecifications = adminConfigurator.Topics;
            int topicsBatchSize = adminConfigurator.TopicsBatchSize;

            services.AddHostedService(provider => new AdminWorker(adminClientConfig, topicSpecifications, topicsBatchSize, provider.GetRequiredService<ILogger<AdminWorker>>()));
        }

        // the broker-reachability tracker the transport feeds and the health-check package reads —
        // TryAdd, so registering both the bus and the check (in any order) shares the one instance.
        services.TryAddSingleton<BusHealth>();

        services.AddSingleton(provider => CreateProducer(provider, producerConfigurator.ProducerConfig));
        services.AddSingleton<IProducer>(provider => new Producer(provider.GetRequiredService<IProducer<Null, byte[]>>(), provider.GetRequiredService<BusHealth>(), provider.GetRequiredService<ILogger<Producer>>()));
        services.AddHostedService<ProducerWorker>();

        if (consumer is not null)
        {
            ConsumerConfigurator consumerConfigurator = new ConsumerConfigurator(services, configuration, producerConfigurator.Messages);

            consumer(consumerConfigurator);
        }

        services.AddSingleton<IBus>(provider => new Bus(provider.GetRequiredService<IProducer>(), producerConfigurator.Messages));

        return services;
    }

    /// <summary>
    /// Creates the shared producer — the container-owned singleton the bus produces through and the
    /// <c>ProducerWorker</c> flushes — with the error/log callbacks wired to the Kafka client logger.
    /// A fatal error stops the application; every other error the client recovers from on its own —
    /// an <c>AllBrokersDown</c> among them, which additionally flips the reachability tracker down.
    /// </summary>
    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger kafkaLogger = provider.GetRequiredService<ILoggerFactory>().CreateLogger(KafkaLogger.Category);
        IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();
        BusHealth health = provider.GetRequiredService<BusHealth>();

        return new ProducerBuilder<Null, byte[]>(configuration)
            .WithClientCallbacks(kafkaLogger, health, lifetime)
            .Build();
    }
}
