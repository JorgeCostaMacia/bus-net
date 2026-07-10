using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain;

public class HostContextTests
{
    private static Transport Transport()
    {
        Dictionary<string, object?> headers = new()
        {
            [TransportHeaders.HostMachineName] = "box-1"u8.ToArray(),
            [TransportHeaders.HostAssembly] = "MyApp"u8.ToArray(),
            [TransportHeaders.HostAssemblyVersion] = "1.2.3.0"u8.ToArray(),
            [TransportHeaders.HostFrameworkVersion] = "10.0.8"u8.ToArray(),
            [TransportHeaders.HostBusVersion] = "2.0.0.0"u8.ToArray(),
            [TransportHeaders.HostOperatingSystemVersion] = "Unix 6.8"u8.ToArray()
        };

        return new Transport(headers, "orders", string.Empty, deliveryTag: 10, redelivered: false);
    }

    [Fact]
    public void CommandContext_ExposesTheHostFromTheHeaders()
    {
        CommandContext<TestCommand> context = new(new TestCommand("pepe"), Transport());

        Assert.Equal("box-1", context.HostMachineName);
        Assert.Equal("MyApp", context.HostAssembly);
        Assert.Equal("1.2.3.0", context.HostAssemblyVersion);
        Assert.Equal("10.0.8", context.HostFrameworkVersion);
        Assert.Equal("2.0.0.0", context.HostBusVersion);
        Assert.Equal("Unix 6.8", context.HostOperatingSystemVersion);
    }

    [Fact]
    public void EventContext_ExposesTheHostFromTheHeaders()
    {
        EventContext<TestEvent> context = new(new TestEvent("pepe"), Transport());

        Assert.Equal("box-1", context.HostMachineName);
        Assert.Equal("MyApp", context.HostAssembly);
        Assert.Equal("2.0.0.0", context.HostBusVersion);
    }
}
