using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using CommandErrorHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors.CommandErrorHandler<JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestCommand, JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestCommandHandler>;
using CommandFaultHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults.CommandFaultHandler<JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestCommand, JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestCommandHandler>;
using EventErrorHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors.EventErrorHandler<JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestEvent, JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestEventSubscriber>;
using EventFaultHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults.EventFaultHandler<JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestEvent, JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes.TestEventSubscriber>;
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
    public void AddCommandAndEvent_SharingAnExchange_Throws()
    {
        // an exchange cannot be direct (commands) and fanout (events) at once — the broker would
        // reject the second declare, so the misconfiguration must surface at registration.
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddBusContext(Configuration(),
            producer => producer
                .AddCommand<TestCommand>("orders")
                .AddEvent<TestEvent>("orders"),
            _ => { }));

        Assert.Contains("orders", exception.Message);
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
        Assert.Equal(2, services.Count(e => e.ServiceType == typeof(IHostedService)));   // the consumer worker + the producer topology declarer

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(CommandErrorHandlerBase));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(CommandFaultHandlerBase));
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
        Assert.Equal(2, services.Count(e => e.ServiceType == typeof(IHostedService)));   // the consumer worker + the producer topology declarer

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(EventErrorHandlerBase));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(EventFaultHandlerBase));
        Assert.Equal(ServiceLifetime.Scoped, faultHandler.Lifetime);
    }
}
