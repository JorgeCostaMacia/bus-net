using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The package's wiring: it lets the send side map its messages through the
/// <see cref="ProducerConfigurator"/> (which binds <c>Bus:Producer</c> and owns the routing map) and
/// the consume side register its handlers through the <see cref="ConsumerConfigurator"/> (which binds
/// <c>Bus:Consumer</c> and reads that map), then registers the <see cref="Bus"/> behind
/// <see cref="IBus"/> — the single owner of the producer (error/log callbacks wired to its logger) and
/// of the routing map. The lifecycle worker is registered before the consumers so it stops last: the
/// consumers stop before the final flush. The bus itself is a lazy singleton, built last.
/// </summary>
internal static class BusInfrastructureContext
{
    /// <summary>
    /// Registers the bus over the application configuration: the producer lambda maps the messages to
    /// topics (its routing map feeds both the bus and the consumers), the consumer lambda registers
    /// the handlers.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> / <c>Bus:Consumer</c> sections.</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their topics.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration, Action<ProducerConfigurator> producer, Action<ConsumerConfigurator> consumer)
    {
        ProducerConfigurator producerConfigurator = new(configuration);

        producer(producerConfigurator);

        services.AddHostedService<BusWorker>();

        ConsumerConfigurator consumerConfigurator = new(services, configuration, producerConfigurator.Messages);

        consumer(consumerConfigurator);

        services.AddSingleton(provider => CreateBus(provider, producerConfigurator.ProducerConfig, producerConfigurator.Messages));
        services.AddSingleton<IBus>(static provider => provider.GetRequiredService<Bus>());

        return services;
    }

    /// <summary>
    /// Creates the bus over the producer it owns — built here with the error/log callbacks wired to
    /// the bus's logger — and the routing map the producer configurator owns.
    /// </summary>
    private static Bus CreateBus(IServiceProvider provider, ProducerConfig configuration, IReadOnlyDictionary<Type, string> messages)
    {
        ILogger kafkaLogger = provider.GetRequiredService<ILoggerFactory>().CreateLogger(KafkaLogger.Category);
        IHostApplicationLifetime lifetime = provider.GetRequiredService<IHostApplicationLifetime>();

        IProducer<Null, byte[]> producer = new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) =>
            {
                KafkaLogger.LogError(kafkaLogger, error);

                if (error.IsFatal) lifetime.StopApplication();
            })
            .SetLogHandler((_, log) => KafkaLogger.Log(kafkaLogger, log))
            .SetStatisticsHandler((_, statistics) => KafkaLogger.LogStatistics(kafkaLogger, statistics))
            .Build();

        return new Bus(producer, messages, provider.GetRequiredService<ILogger<Bus>>());
    }
}
