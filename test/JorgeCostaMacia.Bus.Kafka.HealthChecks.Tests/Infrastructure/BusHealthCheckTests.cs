using JorgeCostaMacia.Bus.Kafka.HealthChecks.Infrastructure;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using BusHealth = JorgeCostaMacia.Bus.Kafka.Infrastructure.BusHealth;

namespace JorgeCostaMacia.Bus.Kafka.HealthChecks.Tests.Infrastructure;

public class BusHealthCheckTests
{
    private readonly BusHealth _health = new BusHealth();

    private static HealthCheckContext Context(IHealthCheck check, HealthStatus? failureStatus = null)
        => new HealthCheckContext() { Registration = new HealthCheckRegistration("bus-kafka", check, failureStatus, tags: null) };

    [Fact]
    public async Task CheckHealthAsync_BrokersReachable_ReportsHealthy()
    {
        BusHealthCheck check = new(_health);

        HealthCheckResult result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Equal("The bus reaches the Kafka brokers.", result.Description);
    }

    [Fact]
    public async Task CheckHealthAsync_AllBrokersDown_ReportsUnhealthyByDefault_WithTheFlipInstant()
    {
        _health.Down();
        BusHealthCheck check = new(_health);

        HealthCheckResult result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("All Kafka brokers are unreachable.", result.Description);
        Assert.Equal(_health.ChangedAt, result.Data["changedAt"]);
    }

    [Fact]
    public async Task CheckHealthAsync_AllBrokersDown_HonorsTheRegistrationFailureStatus()
    {
        _health.Down();
        BusHealthCheck check = new(_health);

        HealthCheckResult result = await check.CheckHealthAsync(Context(check, HealthStatus.Degraded), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
