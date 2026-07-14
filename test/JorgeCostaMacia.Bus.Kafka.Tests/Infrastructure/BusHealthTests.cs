using JorgeCostaMacia.Bus.Kafka.Infrastructure;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure;

public class BusHealthTests
{
    [Fact]
    public void StartsUp_WithAConstructionTimestamp()
    {
        DateTime before = DateTime.UtcNow;

        BusHealth health = new BusHealth();

        Assert.True(health.IsUp);
        Assert.InRange(health.ChangedAt, before, DateTime.UtcNow);
    }

    [Fact]
    public void Down_FlipsTheState_AndStampsTheFlip()
    {
        BusHealth health = new BusHealth();
        DateTime constructedAt = health.ChangedAt;

        health.Down();

        Assert.False(health.IsUp);
        Assert.True(health.ChangedAt >= constructedAt);
    }

    [Fact]
    public void Down_AlreadyDown_KeepsTheFirstFlipInstant()
    {
        BusHealth health = new BusHealth();
        health.Down();
        DateTime flippedAt = health.ChangedAt;

        health.Down();

        Assert.False(health.IsUp);
        Assert.Equal(flippedAt, health.ChangedAt);
    }

    [Fact]
    public void Up_AfterDown_FlipsBack_AndStampsTheFlip()
    {
        BusHealth health = new BusHealth();
        health.Down();
        DateTime downAt = health.ChangedAt;

        health.Up();

        Assert.True(health.IsUp);
        Assert.True(health.ChangedAt >= downAt);
    }

    [Fact]
    public void Up_AlreadyUp_KeepsTheConstructionInstant()
    {
        BusHealth health = new BusHealth();
        DateTime constructedAt = health.ChangedAt;

        health.Up();

        Assert.True(health.IsUp);
        Assert.Equal(constructedAt, health.ChangedAt);
    }
}
