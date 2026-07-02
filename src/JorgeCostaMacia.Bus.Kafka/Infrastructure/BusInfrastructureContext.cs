using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Registers the bus infrastructure: the global configurations, the shared producer (error/log
/// callbacks wired to the logger) with its lifecycle worker — registered first so it stops last, the
/// consumers stop before the final flush — and the <see cref="Bus"/> behind its facades.
/// </summary>
internal static class BusInfrastructureContext
{
    /// <summary>Registers the bus infrastructure over the global configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The global configuration (connection + producer/admin tuning).</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, BusConfiguration configuration)
    {
        services.AddSingleton<IProducerConfiguration>(configuration);

        services.AddSingleton(CreateProducer);

        services.AddHostedService<BusProducer>();

        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        return services;
    }

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider)
    {
        IProducerConfiguration configuration = provider.GetRequiredService<IProducerConfiguration>();
        ILogger<BusProducer> logger = provider.GetRequiredService<ILogger<BusProducer>>();

        return new ProducerBuilder<Null, byte[]>(configuration.ProducerConfig)
            .SetErrorHandler((_, error) =>
            {
                using (logger.BeginScope(new Dictionary<string, object?>
                {
                    ["@Error"] = error
                }))
                {
                    logger.LogError("Producer error.");
                }
            })
            .SetLogHandler((_, log) =>
            {
                using (logger.BeginScope(new Dictionary<string, object?>
                {
                    ["Name"] = log.Name,
                    ["Facility"] = log.Facility
                }))
                {
                    logger.Log((LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging), "{Message}", log.Message);
                }
            })
            .Build();
    }
}
