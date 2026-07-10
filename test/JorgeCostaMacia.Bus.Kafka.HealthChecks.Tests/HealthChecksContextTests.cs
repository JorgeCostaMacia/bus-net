using JorgeCostaMacia.Bus.Kafka.HealthChecks.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using BusHealth = JorgeCostaMacia.Bus.Kafka.Infrastructure.BusHealth;

namespace JorgeCostaMacia.Bus.Kafka.HealthChecks.Tests;

public class HealthChecksContextTests
{
    private static ServiceProvider Provider(Action<IHealthChecksBuilder> add)
    {
        ServiceCollection services = [];

        services.AddSingleton<BusHealth>(new BusHealth());
        add(services.AddHealthChecks());

        return services.BuildServiceProvider();
    }

    private static HealthCheckRegistration Registration(ServiceProvider provider)
        => Assert.Single(provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations);

    [Fact]
    public void AddKafkaBus_Defaults_RegistersTheCheckUnderItsDefaultName()
    {
        ServiceProvider provider = Provider(builder => builder.AddKafkaBus());

        HealthCheckRegistration registration = Registration(provider);
        Assert.Equal("bus-kafka", registration.Name);
        Assert.Equal(HealthStatus.Unhealthy, registration.FailureStatus);
        Assert.Empty(registration.Tags);
    }

    [Fact]
    public void AddKafkaBus_Custom_RegistersTheNameStatusAndTags()
    {
        ServiceProvider provider = Provider(builder => builder.AddKafkaBus("orders-bus", HealthStatus.Degraded, ["ready"]));

        HealthCheckRegistration registration = Registration(provider);
        Assert.Equal("orders-bus", registration.Name);
        Assert.Equal(HealthStatus.Degraded, registration.FailureStatus);
        Assert.Equal("ready", Assert.Single(registration.Tags));
    }

    [Fact]
    public void AddKafkaBus_Factory_BuildsTheCheckOverTheBusTracker()
    {
        ServiceProvider provider = Provider(builder => builder.AddKafkaBus());

        Assert.IsType<BusHealthCheck>(Registration(provider).Factory(provider));
    }
}
