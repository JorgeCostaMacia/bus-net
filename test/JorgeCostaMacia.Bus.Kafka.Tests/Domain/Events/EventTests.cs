using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class EventTests
{
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
