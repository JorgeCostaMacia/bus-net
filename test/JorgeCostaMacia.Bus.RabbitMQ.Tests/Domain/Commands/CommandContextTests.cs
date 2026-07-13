using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain.Commands;

public class CommandContextTests
{
    private static readonly Guid CONVERSATION_ID = Guid.NewGuid();
    private static readonly Guid AGGREGATE_ID = Guid.NewGuid();
    private static readonly Guid AGGREGATE_CORRELATION_ID = Guid.NewGuid();
    private static readonly DateTime OCCURRED_AT = new(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

    private static Transport Transport()
    {
        Dictionary<string, object?> headers = new Dictionary<string, object?>()
        {
            [TransportHeaders.ConversationId] = TransportHeaders.ToHeader(CONVERSATION_ID),
            [TransportHeaders.ConversationAddress] = TransportHeaders.ToHeader("orders"),
            [TransportHeaders.ConversationOccurredAt] = TransportHeaders.ToHeader(OCCURRED_AT.ToString("O")),
            [TransportHeaders.AggregateId] = TransportHeaders.ToHeader(AGGREGATE_ID),
            [TransportHeaders.AggregateCorrelationId] = TransportHeaders.ToHeader(AGGREGATE_CORRELATION_ID),
            [TransportHeaders.AggregateOccurredAt] = TransportHeaders.ToHeader(OCCURRED_AT.ToString("O")),
            [TransportHeaders.RetryCount] = TransportHeaders.ToHeader(2),
            [TransportHeaders.HostMachineName] = TransportHeaders.ToHeader("box-1"),
            [TransportHeaders.HostAssembly] = TransportHeaders.ToHeader("MyApp"),
            [TransportHeaders.HostAssemblyVersion] = TransportHeaders.ToHeader("1.2.3.0"),
            [TransportHeaders.HostFrameworkVersion] = TransportHeaders.ToHeader("10.0.8"),
            [TransportHeaders.HostBusVersion] = TransportHeaders.ToHeader("2.0.0.0"),
            [TransportHeaders.HostOperatingSystemVersion] = TransportHeaders.ToHeader("Unix 6.8")
        };

        return new Transport(headers, "orders", string.Empty, deliveryTag: 10, redelivered: false);
    }

    private static CommandContext<TestCommand> CreateSut() => new(new TestCommand("pepe"), Transport());

    [Fact]
    public void MessageAndTransport_AreTheDeliveredPair()
    {
        TestCommand command = new("pepe");
        Transport transport = Transport();

        CommandContext<TestCommand> context = new(command, transport);

        Assert.Same(command, context.Message);
        Assert.Same(transport, context.Transport);
    }

    [Fact]
    public void AggregateTrace_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal(AGGREGATE_ID, context.AggregateId);
        Assert.Equal(AGGREGATE_CORRELATION_ID, context.AggregateCorrelationId);
        Assert.Equal(OCCURRED_AT, context.AggregateOccurredAt);
    }

    [Fact]
    public void Conversation_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal(CONVERSATION_ID, context.ConversationId);
        Assert.Equal("orders", context.ConversationAddress);
        Assert.Equal(OCCURRED_AT, context.ConversationOccurredAt);
    }

    [Fact]
    public void RetryCount_ReadsFromTheTransportHeaders()
        => Assert.Equal(2, CreateSut().RetryCount);

    [Fact]
    public void Host_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal("box-1", context.HostMachineName);
        Assert.Equal("MyApp", context.HostAssembly);
        Assert.Equal("1.2.3.0", context.HostAssemblyVersion);
        Assert.Equal("10.0.8", context.HostFrameworkVersion);
        Assert.Equal("2.0.0.0", context.HostBusVersion);
        Assert.Equal("Unix 6.8", context.HostOperatingSystemVersion);
    }
}
