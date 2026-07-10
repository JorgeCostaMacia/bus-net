using JorgeCostaMacia.Bus.Kafka.HealthChecks.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using BusHealth = JorgeCostaMacia.Bus.Kafka.Infrastructure.BusHealth;

namespace JorgeCostaMacia.Bus.Kafka.HealthChecks;

/// <summary>
/// The package's registration facade: plugs the bus's health check onto ASP.NET Core's health-check
/// pipeline. The check reports whether the bus reaches the Kafka brokers — call it after
/// <c>AddBusContext</c>, which registers the reachability tracker it reads.
/// </summary>
public static class HealthChecksContext
{
    /// <summary>
    /// Registers the Kafka bus health check: healthy while the bus reaches the brokers (an untouched
    /// bus counts as reachable — nothing observed yet), the failure status once the client reports
    /// every broker down, until a successful produce or consumed delivery flips it back. Tag it
    /// (typically <c>"ready"</c>) to pick which health endpoints run it.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check's name, <c>bus-kafka</c> by default.</param>
    /// <param name="failureStatus">The status reported when every broker is unreachable, or <see langword="null"/> for <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">The tags the health endpoints filter on (typically <c>"ready"</c> for readiness), or <see langword="null"/> for none.</param>
    /// <returns>The same builder, to allow method chaining.</returns>
    public static IHealthChecksBuilder AddKafkaBus(this IHealthChecksBuilder builder, string name = "bus-kafka", HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(name, provider => new BusHealthCheck(provider.GetRequiredService<BusHealth>()), failureStatus, tags));
}
