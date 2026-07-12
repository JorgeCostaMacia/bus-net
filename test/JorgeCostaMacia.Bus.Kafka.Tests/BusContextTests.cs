using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class BusContextTests
{
    private static IConfiguration Configuration(bool producer = true, bool consumer = false, string? producerBootstrap = "bus:9092", string? producerUser = "user")
    {
        Dictionary<string, string?> values = [];

        if (producer)
        {
            values["Bus:Producer:BootstrapServers"] = producerBootstrap;
            values["Bus:Producer:SaslUsername"] = producerUser;
            values["Bus:Producer:SaslPassword"] = "pass";
        }

        if (consumer)
        {
            values["Bus:Consumer:BootstrapServers"] = "bus:9092";
            values["Bus:Consumer:SaslUsername"] = "user";
            values["Bus:Consumer:SaslPassword"] = "pass";
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact]
    public void AddBusContext_MissingProducerSection_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(producer: false), _ => { }, _ => { }));

        Assert.Contains("Bus:Producer", exception.Message);
    }

    [Fact]
    public void AddBusContext_MissingBootstrapServers_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(producerBootstrap: null), _ => { }, _ => { }));

        Assert.Contains("BootstrapServers", exception.Message);
    }

    [Fact]
    public void AddBusContext_MissingSaslUsername_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(producerUser: null), _ => { }, _ => { }));

        Assert.Contains("SaslUsername", exception.Message);
    }

    [Fact]
    public void AddBusContext_ProducerOnly_RegistersTheSendSide_AndNeedsNoConsumerSection()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(), _ => { });

        Assert.Contains(services, e => e.ServiceType == typeof(IProducer<Null, byte[]>));
        Assert.Contains(services, e => e.ServiceType == typeof(IProducer));
        Assert.Contains(services, e => e.ServiceType == typeof(IHostedService) && e.ImplementationType == typeof(ProducerWorker));
        Assert.Contains(services, e => e.ServiceType == typeof(IBus));
        Assert.DoesNotContain(services, e => e.ServiceType == typeof(TestCommandHandler));
    }

    [Fact]
    public void AddBusContext_ConsumerSectionWithoutBootstrap_Throws()
    {
        Dictionary<string, string?> values = new()
        {
            ["Bus:Producer:BootstrapServers"] = "bus:9092",
            ["Bus:Producer:SaslUsername"] = "user",
            ["Bus:Producer:SaslPassword"] = "pass",
            ["Bus:Consumer:SaslUsername"] = "user"
        };
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(configuration, _ => { }, _ => { }));

        Assert.Contains("Bus:Consumer", exception.Message);
    }

    [Fact]
    public void AddBusContext_ConsumerMissingSaslPassword_Throws()
    {
        Dictionary<string, string?> values = new()
        {
            ["Bus:Producer:BootstrapServers"] = "bus:9092",
            ["Bus:Producer:SaslUsername"] = "user",
            ["Bus:Producer:SaslPassword"] = "pass",
            ["Bus:Consumer:BootstrapServers"] = "bus:9092",
            ["Bus:Consumer:SaslUsername"] = "user"
        };
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(configuration, _ => { }, _ => { }));

        Assert.Contains("SaslPassword", exception.Message);
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
    public void AddEvent_DuplicateType_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddBusContext(Configuration(),
            producer => producer
                .AddEvent<TestEvent>("orders.created")
                .AddEvent<TestEvent>("other"),
            _ => { }));

        Assert.Contains(nameof(TestEvent), exception.Message);
    }

    [Fact]
    public void AddCommandHandler_WithoutTheCommandMapped_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(consumer: true),
                _ => { },
                consumer => consumer.AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler")));

        Assert.Contains(nameof(TestCommand), exception.Message);
    }

    [Fact]
    public void AddCommandHandler_WithoutConsumerSection_Throws()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(),
                producer => producer.AddCommand<TestCommand>("orders"),
                consumer => consumer.AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler")));

        Assert.Contains("Bus:Consumer", exception.Message);
    }

    [Fact]
    public void AddCommandHandler_DuplicateGroupId_Throws()
    {
        // like the message → topic map, the group-id registry rejects duplicates at registration:
        // two consumers sharing a group id also share the machine-name group.instance.id and fence
        // each other out of the group at startup.
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddBusContext(Configuration(consumer: true),
                producer => producer.AddCommand<TestCommand>("orders"),
                consumer => consumer
                    .AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler")
                    .AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler")));

        Assert.Contains("orders.handler", exception.Message);
    }

    [Fact]
    public void AddCommandHandler_RegistersTheHandlerAndItsWorker()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(consumer: true),
            producer => producer.AddCommand<TestCommand>("orders"),
            consumer => consumer.AddCommandHandler<TestCommand, TestCommandHandler>("orders.handler"));

        ServiceDescriptor handler = Assert.Single(services, e => e.ServiceType == typeof(TestCommandHandler));
        Assert.Equal(ServiceLifetime.Scoped, handler.Lifetime);
        Assert.Equal(2, services.Count(e => e.ServiceType == typeof(IHostedService)));

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(CommandErrorHandlerBase<TestCommand, TestCommandHandler>));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(CommandFaultHandlerBase<TestCommand, TestCommandHandler>));
        Assert.Equal(ServiceLifetime.Scoped, faultHandler.Lifetime);
    }

    [Fact]
    public void AddEventSubscriber_RegistersTheSubscriberAndItsWorker()
    {
        ServiceCollection services = [];

        services.AddBusContext(Configuration(consumer: true),
            producer => producer.AddEvent<TestEvent>("orders.created"),
            consumer => consumer.AddEventSubscriber<TestEvent, TestEventSubscriber>("billing.on.orders.created.subscriber"));

        Assert.Single(services, e => e.ServiceType == typeof(TestEventSubscriber));
        Assert.Equal(2, services.Count(e => e.ServiceType == typeof(IHostedService)));

        ServiceDescriptor errorHandler = Assert.Single(services, e => e.ServiceType == typeof(EventErrorHandlerBase<TestEvent, TestEventSubscriber>));
        Assert.Equal(ServiceLifetime.Scoped, errorHandler.Lifetime);
        ServiceDescriptor faultHandler = Assert.Single(services, e => e.ServiceType == typeof(EventFaultHandlerBase<TestEvent, TestEventSubscriber>));
        Assert.Equal(ServiceLifetime.Scoped, faultHandler.Lifetime);
    }
}
