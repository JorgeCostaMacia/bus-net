using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Registers the bus infrastructure: the shared producer (configured from the <c>Bus:Producer</c>
/// section over the defaults, error/log callbacks wired to the logger) with its lifecycle worker —
/// registered first so it stops last, the consumers stop before the final flush — and the
/// <see cref="Bus"/> behind its facades.
/// </summary>
internal static class BusInfrastructureContext
{
    private const string PRODUCER_SECTION = "Bus:Producer";

    /// <summary>Registers the bus infrastructure over the application configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> section.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration)
    {
        ProducerConfig producer = CreateProducerConfig(configuration);

        services.AddSingleton(provider => CreateProducer(provider, producer));

        services.AddHostedService<BusProducer>();

        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        return services;
    }

    /// <summary>
    /// Maps the <c>Bus:Producer</c> section onto a <see cref="BusConfiguration"/> (the curated
    /// setting surface; unset values fall back to the defaults when it composes the producer
    /// configuration).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The assembled producer configuration.</returns>
    /// <exception cref="InvalidOperationException"><c>Bus:Producer:BootstrapServers</c> is missing.</exception>
    internal static ProducerConfig CreateProducerConfig(IConfiguration configuration)
    {
        BusConfiguration bus = configuration.GetSection(PRODUCER_SECTION).Get<BusConfiguration>() ?? new BusConfiguration();

        if (string.IsNullOrWhiteSpace(bus.BootstrapServers))
        {
            throw new InvalidOperationException($"'{PRODUCER_SECTION}:{nameof(bus.BootstrapServers)}' is null.");
        }

        return bus.ProducerConfig;
    }

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger<BusProducer> logger = provider.GetRequiredService<ILogger<BusProducer>>();

        return new ProducerBuilder<Null, byte[]>(configuration)
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
