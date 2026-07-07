using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using IBus = JorgeCostaMacia.Bus.RabbitMQ.Domain.IBus;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class BusContextTests
{
    private static IConfiguration Configuration(bool connection = true, string? hostName = "bus", string? userName = "user", string? password = "pass")
    {
        Dictionary<string, string?> values = [];

        if (connection)
        {
            values["Bus:Connection:HostName"] = hostName;
            values["Bus:Connection:UserName"] = userName;
            values["Bus:Connection:Password"] = password;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact]
    public void AddBusContext_MissingConnectionSection_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(connection: false), _ => { }, _ => { }));

        Assert.Contains("Bus:Connection", exception.Message);
    }

    [Fact]
    public void AddBusContext_MissingHostName_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(hostName: null), _ => { }, _ => { }));

        Assert.Contains("HostName", exception.Message);
    }

    [Fact]
    public void AddBusContext_MissingUserName_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(userName: null), _ => { }, _ => { }));

        Assert.Contains("UserName", exception.Message);
    }

    [Fact]
    public void AddBusContext_MissingPassword_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(password: null), _ => { }, _ => { }));

        Assert.Contains("Password", exception.Message);
    }

    [Fact]
    public void AddBusContext_ProducerOnly_RegistersTheSendSide_AndNeedsNoConsumer()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(), _ => { });

        Assert.Contains(services, e => e.ServiceType == typeof(IConnection));
        Assert.Contains(services, e => e.ServiceType == typeof(IProducer));
        Assert.Contains(services, e => e.ServiceType == typeof(IBus));
        Assert.DoesNotContain(services, e => e.ServiceType == typeof(TestCommandHandler));
    }

    [Fact]
    public void AddCommand_DuplicateType_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddBusContext(Configuration(),
            producer => producer
                .AddCommand<TestCommand>("orders")
                .AddCommand<TestCommand>("other"),
            _ => { }));

        Assert.Contains(nameof(TestCommand), exception.Message);
    }

    [Fact]
    public void AddCommandHandler_WithoutTheCommandMapped_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(),
                _ => { },
                consumer => consumer.AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler")));

        Assert.Contains(nameof(TestCommand), exception.Message);
    }

    [Fact]
    public void AddCommandHandler_RegistersTheHandlerAndItsWorker()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(),
            producer => producer.AddCommand<TestCommand>("orders"),
            consumer => consumer.AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler"));

        ServiceDescriptor handler = Assert.Single(services, e => e.ServiceType == typeof(TestCommandHandler));
        Assert.Equal(ServiceLifetime.Scoped, handler.Lifetime);
        Assert.Single(services, e => e.ServiceType == typeof(IHostedService));

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(Domain.Commands.Errors.CommandErrorHandler<TestCommand, TestCommandHandler>));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(Domain.Commands.Faults.CommandFaultHandler<TestCommand, TestCommandHandler>));
        Assert.Equal(ServiceLifetime.Scoped, faultHandler.Lifetime);
    }

    [Fact]
    public void AddEventSubscriber_RegistersTheSubscriberAndItsWorker()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(),
            producer => producer.AddEvent<TestEvent>("orders.created"),
            consumer => consumer.AddEventSubscriber<TestEvent, TestEventSubscriber>("billing.on.orders.created.subscriber"));

        Assert.Single(services, e => e.ServiceType == typeof(TestEventSubscriber));
        Assert.Single(services, e => e.ServiceType == typeof(IHostedService));

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(Domain.Events.Errors.EventErrorHandler<TestEvent, TestEventSubscriber>));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(Domain.Events.Faults.EventFaultHandler<TestEvent, TestEventSubscriber>));
        Assert.Equal(ServiceLifetime.Scoped, faultHandler.Lifetime);
    }
}
