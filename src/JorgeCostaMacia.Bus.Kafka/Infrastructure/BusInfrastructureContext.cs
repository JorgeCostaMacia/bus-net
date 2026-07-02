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
    /// <summary>Registers the bus infrastructure over the application configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> section.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration)
    {
        ProducerConfig producer = KafkaProducerConfiguration.Create(configuration).ProducerConfig;

        services.AddSingleton(provider => CreateProducer(provider, producer));

        services.AddHostedService<BusProducer>();

        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        return services;
    }

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger<BusProducer> logger = provider.GetRequiredService<ILogger<BusProducer>>();

        return new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => KafkaProducerLogger.LogError(logger, error))
            .SetLogHandler((_, log) => KafkaProducerLogger.Log(logger, log))
            .Build();
    }
}
