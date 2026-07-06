using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class HostContextTests
{
    private static Transport Transport()
    {
        Headers headers =
        [
            new Header(TransportHeaders.HostMachineName, "box-1"u8.ToArray()),
            new Header(TransportHeaders.HostAssembly, "MyApp"u8.ToArray()),
            new Header(TransportHeaders.HostAssemblyVersion, "1.2.3.0"u8.ToArray()),
            new Header(TransportHeaders.HostFrameworkVersion, "10.0.8"u8.ToArray()),
            new Header(TransportHeaders.HostBusVersion, "2.0.0.0"u8.ToArray()),
            new Header(TransportHeaders.HostOperatingSystemVersion, "Unix 6.8"u8.ToArray())
        ];

        return new Transport(headers.ToImmutableList(), "orders", new Partition(0), new Offset(10), null, new Timestamp(DateTime.UtcNow));
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
