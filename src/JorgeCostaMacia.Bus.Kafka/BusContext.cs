using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;
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
    /// this service sends/publishes to their topics; the optional consumer lambda registers its
    /// handlers — omit it for a send-only service, which then needs no <c>Bus:Consumer</c> section.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> section (and <c>Bus:Consumer</c> when a consumer lambda is supplied).</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their topics.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers, or <see langword="null"/> for a send-only service.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusContext(this IServiceCollection services, IConfiguration configuration, Action<ProducerConfigurator> producer, Action<ConsumerConfigurator>? consumer = null)
        => services.AddBusInfrastructureContext(configuration, producer, consumer);
}
