using System.Text.Json;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class MessageRecordTests
{
    [Fact]
    public void Command_RoundTrips_ThroughTheSerializer()
    {
        TestCommand command = new("pepe", Guid.NewGuid(), Guid.NewGuid(), new DateTime(2026, 7, 3, 10, 0, 0, DateTimeKind.Utc), ["g1", "g2"]);

        TestCommand roundTripped = JsonSerializer.Deserialize<TestCommand>(JsonSerializer.SerializeToUtf8Bytes(command))!;

        Assert.Equal(command.Name, roundTripped.Name);
        Assert.Equal(command.AggregateId, roundTripped.AggregateId);
        Assert.Equal(command.AggregateCorrelationId, roundTripped.AggregateCorrelationId);
        Assert.Equal(command.AggregateOccurredAt, roundTripped.AggregateOccurredAt);
        Assert.Equal(command.AggregateConsumers, roundTripped.AggregateConsumers);
    }


    [Fact]
    public void Command_Defaults_GenerateTheTrace()
    {
        DateTime before = DateTime.UtcNow;

        TestCommand command = new("pepe");

        Assert.NotEqual(Guid.Empty, command.AggregateId);
        Assert.Equal(command.AggregateId, command.AggregateCorrelationId);
        Assert.InRange(command.AggregateOccurredAt, before, DateTime.UtcNow);
        Assert.Empty(command.AggregateConsumers);
    }

    [Fact]
    public void Command_SuppliedValues_AreKept()
    {
        Guid id = Guid.NewGuid();
        Guid correlation = Guid.NewGuid();
        DateTime occurredAt = new(2026, 7, 3, 10, 0, 0, DateTimeKind.Utc);

        TestCommand command = new("pepe", id, correlation, occurredAt, ["g1"]);

        Assert.Equal(id, command.AggregateId);
        Assert.Equal(correlation, command.AggregateCorrelationId);
        Assert.Equal(occurredAt, command.AggregateOccurredAt);
        Assert.Equal(["g1"], command.AggregateConsumers);
    }

    [Fact]
    public void Event_Defaults_GenerateTheTrace()
    {
        TestEvent @event = new("pepe");

        Assert.NotEqual(Guid.Empty, @event.AggregateId);
        Assert.Equal(@event.AggregateId, @event.AggregateCorrelationId);
        Assert.Empty(@event.AggregateConsumers);
    }

    [Fact]
    public void Event_SuppliedCorrelation_IsKept()
    {
        Guid correlation = Guid.NewGuid();

        TestEvent @event = new("pepe", aggregateCorrelationId: correlation);

        Assert.Equal(correlation, @event.AggregateCorrelationId);
        Assert.NotEqual(@event.AggregateId, @event.AggregateCorrelationId);
    }
}
