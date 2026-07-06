using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests;

public class RetryContextTests
{
    [Fact]
    public void AddRetryContext_RegistersTheQuartzRetryScheduler_AsSingleton()
    {
        ServiceCollection services = [];

        services.AddRetryContext();

        ServiceDescriptor scheduler = Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IRetryScheduler));
        Assert.Equal(typeof(RetryScheduler), scheduler.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, scheduler.Lifetime);
    }
}
