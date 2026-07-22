using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain.Events;

public class EventContextTests
{
    private static readonly Guid ConversationId = Guid.NewGuid();
    private static readonly Guid AggregateId = Guid.NewGuid();
    private static readonly Guid AggregateCorrelationId = Guid.NewGuid();
    private static readonly DateTime OccurredAt = new DateTime(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

    private static Transport Transport()
    {
        Dictionary<string, object?> headers = new Dictionary<string, object?>()
        {
            [TransportHeaders.ConversationId] = TransportHeaders.ToHeader(ConversationId),
            [TransportHeaders.ConversationAddress] = TransportHeaders.ToHeader("orders.created"),
            [TransportHeaders.ConversationOccurredAt] = TransportHeaders.ToHeader(OccurredAt.ToString("O")),
            [TransportHeaders.AggregateId] = TransportHeaders.ToHeader(AggregateId),
            [TransportHeaders.AggregateCorrelationId] = TransportHeaders.ToHeader(AggregateCorrelationId),
            [TransportHeaders.AggregateOccurredAt] = TransportHeaders.ToHeader(OccurredAt.ToString("O")),
            [TransportHeaders.AggregateConsumers] = TransportHeaders.ToHeader(new string[] { "g1", "g2" }),
            [TransportHeaders.RetryCount] = TransportHeaders.ToHeader(3),
            [TransportHeaders.HostMachineName] = TransportHeaders.ToHeader("box-1"),
            [TransportHeaders.HostAssembly] = TransportHeaders.ToHeader("MyApp"),
            [TransportHeaders.HostAssemblyVersion] = TransportHeaders.ToHeader("1.2.3.0"),
            [TransportHeaders.HostFrameworkVersion] = TransportHeaders.ToHeader("10.0.8"),
            [TransportHeaders.HostBusVersion] = TransportHeaders.ToHeader("2.0.0.0"),
            [TransportHeaders.HostOperatingSystemVersion] = TransportHeaders.ToHeader("Unix 6.8")
        };

        return new Transport(headers, "orders.created", string.Empty, deliveryTag: 10, redelivered: false);
    }

    private static EventContext<TestEvent> CreateSut() => new EventContext<TestEvent>(new TestEvent("pepe"), Transport());

    [Fact]
    public void MessageAndTransport_AreTheDeliveredPair()
    {
        TestEvent @event = new TestEvent("pepe");
        Transport transport = Transport();

        EventContext<TestEvent> context = new EventContext<TestEvent>(@event, transport);

        Assert.Same(@event, context.Message);
        Assert.Same(transport, context.Transport);
    }

    [Fact]
    public void AggregateTrace_ReadsFromTheTransportHeaders()
    {
        EventContext<TestEvent> context = CreateSut();

        Assert.Equal(AggregateId, context.AggregateId);
        Assert.Equal(AggregateCorrelationId, context.AggregateCorrelationId);
        Assert.Equal(OccurredAt, context.AggregateOccurredAt);
    }

    [Fact]
    public void Conversation_ReadsFromTheTransportHeaders()
    {
        EventContext<TestEvent> context = CreateSut();

        Assert.Equal(ConversationId, context.ConversationId);
        Assert.Equal("orders.created", context.ConversationAddress);
        Assert.Equal(OccurredAt, context.ConversationOccurredAt);
    }

    [Fact]
    public void RetryCount_ReadsFromTheTransportHeaders()
        => Assert.Equal(3, CreateSut().RetryCount);

    [Fact]
    public void AggregateConsumers_ReadsTheTargetsFromTheTransportHeaders()
        => Assert.Equal(new string[] { "g1", "g2" }, CreateSut().AggregateConsumers);

    [Fact]
    public void Host_ReadsFromTheTransportHeaders()
    {
        EventContext<TestEvent> context = CreateSut();

        Assert.Equal("box-1", context.HostMachineName);
        Assert.Equal("MyApp", context.HostAssembly);
        Assert.Equal("1.2.3.0", context.HostAssemblyVersion);
        Assert.Equal("10.0.8", context.HostFrameworkVersion);
        Assert.Equal("2.0.0.0", context.HostBusVersion);
        Assert.Equal("Unix 6.8", context.HostOperatingSystemVersion);
    }
}
