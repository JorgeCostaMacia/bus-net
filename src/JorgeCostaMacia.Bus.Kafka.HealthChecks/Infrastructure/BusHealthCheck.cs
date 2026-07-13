using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace JorgeCostaMacia.Bus.Kafka.HealthChecks.Infrastructure;

/// <summary>
/// The health check over the bus's broker-reachability tracker — no probe connection, no extra I/O:
/// it reads the state the transport already feeds (the client's <c>AllBrokersDown</c> flips it down,
/// any successful produce or consumed delivery flips it back up). Healthy while the brokers are
/// reachable — an untouched bus counts as reachable; when every broker is down it reports the
/// registration's failure status, with the UTC instant of the flip under <c>changedAt</c> in the
/// result's data. Internal: registered through <see cref="HealthChecksContext.AddKafkaBus"/>, whose
/// factory resolves the internal tracker.
/// </summary>
internal sealed class BusHealthCheck : IHealthCheck
{
    private readonly BusHealth _health;

    /// <summary>Creates the check over the bus's broker-reachability tracker.</summary>
    /// <param name="health">The tracker the transport feeds.</param>
    public BusHealthCheck(BusHealth health)
    {
        _health = health;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => Task.FromResult(_health.IsUp
            ? HealthCheckResult.Healthy("The bus reaches the Kafka brokers.")
            : new HealthCheckResult(context.Registration.FailureStatus, "All Kafka brokers are unreachable.", data: new Dictionary<string, object> { ["changedAt"] = _health.ChangedAt }));
}
