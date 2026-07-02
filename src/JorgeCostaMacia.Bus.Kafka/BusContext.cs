using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.Kafka;

/// <summary>
/// The package's registration facade: registers the Kafka bus (infrastructure + producer lifecycle)
/// and lets each context map its messages and handlers through the <see cref="BusConfigurator"/>.
/// The connection is declared once in the <c>Bus:Producer</c> configuration section and never
/// repeated.
/// </summary>
public static class BusContext
{
    /// <summary>
    /// Registers the bus over the application configuration and lets each context map its messages
    /// and handlers through the configurator.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> section.</param>
    /// <param name="configure">Maps the contexts' messages and handlers (e.g. chained <c>Map*BusContext</c> calls).</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusContext(this IServiceCollection services, IConfiguration configuration, Action<BusConfigurator> configure)
    {
        services.AddBusInfrastructureContext(configuration);

        configure(new BusConfigurator(services, configuration));

        return services;
    }
}
