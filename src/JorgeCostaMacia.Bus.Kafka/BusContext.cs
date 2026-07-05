using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.Kafka;

/// <summary>
/// The package's registration facade: registers the bus over the <c>Bus:Producer</c> /
/// <c>Bus:Consumer</c> configuration sections. The send side maps its messages to topics through the
/// <see cref="ProducerConfigurator"/>; the consume side registers its handlers through the
/// <see cref="ConsumerConfigurator"/> — the producer lambda runs first, so the topics its handlers
/// resolve are already mapped.
/// </summary>
public static class BusContext
{
    /// <summary>
    /// Registers the bus over the application configuration: the producer lambda maps the messages
    /// this service sends/publishes to their topics; the consumer lambda registers its handlers.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> / <c>Bus:Consumer</c> sections.</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their topics.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusContext(this IServiceCollection services, IConfiguration configuration, Action<ProducerConfigurator> producer, Action<ConsumerConfigurator> consumer)
        => services.AddBusInfrastructureContext(configuration, producer, consumer);
}
