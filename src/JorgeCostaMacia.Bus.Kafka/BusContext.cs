using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.Kafka;

/// <summary>
/// The package's registration facade: registers the bus over the <c>Bus:Producer</c> /
/// <c>Bus:Consumer</c> configuration sections and lets each context map its messages and handlers
/// through the <see cref="BusContextConfigurator"/>.
/// </summary>
public static class BusContext
{
    /// <summary>
    /// Registers the bus over the application configuration and lets each context map its messages
    /// and handlers through the configurator.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> / <c>Bus:Consumer</c> sections.</param>
    /// <param name="configure">Maps the contexts' messages and handlers (e.g. chained <c>Map*BusContext</c> calls).</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusContext(this IServiceCollection services, IConfiguration configuration, Action<BusContextConfigurator> configure)
        => services.AddBusInfrastructureContext(configuration, configure);
}
