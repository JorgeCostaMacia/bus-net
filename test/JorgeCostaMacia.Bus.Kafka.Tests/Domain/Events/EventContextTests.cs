using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Domain.Events;

public class EventContextTests
{
    private static readonly Guid CONVERSATION_ID = Guid.NewGuid();
    private static readonly Guid AGGREGATE_ID = Guid.NewGuid();
    private static readonly Guid AGGREGATE_CORRELATION_ID = Guid.NewGuid();
    private static readonly DateTime OCCURRED_AT = new(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

    private static Transport Transport()
    {
        Headers headers = new Headers
        {
            new Header(TransportHeaders.ConversationId, TransportHeaders.ToHeader(CONVERSATION_ID)),
            new Header(TransportHeaders.ConversationAddress, TransportHeaders.ToHeader("orders.created")),
            new Header(TransportHeaders.ConversationOccurredAt, TransportHeaders.ToHeader(OCCURRED_AT.ToString("O"))),
            new Header(TransportHeaders.AggregateId, TransportHeaders.ToHeader(AGGREGATE_ID)),
            new Header(TransportHeaders.AggregateCorrelationId, TransportHeaders.ToHeader(AGGREGATE_CORRELATION_ID)),
            new Header(TransportHeaders.AggregateOccurredAt, TransportHeaders.ToHeader(OCCURRED_AT.ToString("O"))),
            new Header(TransportHeaders.AggregateConsumers, TransportHeaders.ToHeader(new[] { "g1", "g2" })),
            new Header(TransportHeaders.RetryCount, TransportHeaders.ToHeader(3)),
            new Header(TransportHeaders.HostMachineName, TransportHeaders.ToHeader("box-1")),
            new Header(TransportHeaders.HostAssembly, TransportHeaders.ToHeader("MyApp")),
            new Header(TransportHeaders.HostAssemblyVersion, TransportHeaders.ToHeader("1.2.3.0")),
            new Header(TransportHeaders.HostFrameworkVersion, TransportHeaders.ToHeader("10.0.8")),
            new Header(TransportHeaders.HostBusVersion, TransportHeaders.ToHeader("2.0.0.0")),
            new Header(TransportHeaders.HostOperatingSystemVersion, TransportHeaders.ToHeader("Unix 6.8"))
        };

        return new Transport(headers.ToImmutableList(), "orders.created", new Partition(0), new Offset(10), null, new Timestamp(OCCURRED_AT));
    }

    private static EventContext<TestEvent> CreateSut() => new(new TestEvent("pepe"), Transport());

    [Fact]
    public void MessageAndTransport_AreTheDeliveredPair()
    {
        TestEvent @event = new("pepe");
        Transport transport = Transport();

        EventContext<TestEvent> context = new(@event, transport);

        Assert.Same(@event, context.Message);
        Assert.Same(transport, context.Transport);
    }

    [Fact]
    public void AggregateTrace_ReadsFromTheTransportHeaders()
    {
        EventContext<TestEvent> context = CreateSut();

        Assert.Equal(AGGREGATE_ID, context.AggregateId);
        Assert.Equal(AGGREGATE_CORRELATION_ID, context.AggregateCorrelationId);
        Assert.Equal(OCCURRED_AT, context.AggregateOccurredAt);
    }

    [Fact]
    public void Conversation_ReadsFromTheTransportHeaders()
    {
        EventContext<TestEvent> context = CreateSut();

        Assert.Equal(CONVERSATION_ID, context.ConversationId);
        Assert.Equal("orders.created", context.ConversationAddress);
        Assert.Equal(OCCURRED_AT, context.ConversationOccurredAt);
    }

    [Fact]
    public void RetryCount_ReadsFromTheTransportHeaders()
        => Assert.Equal(3, CreateSut().RetryCount);

    [Fact]
    public void AggregateConsumers_ReadsTheTargetsFromTheTransportHeaders()
        => Assert.Equal(new[] { "g1", "g2" }, CreateSut().AggregateConsumers);

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
