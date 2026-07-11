using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ConsumerConfigurationTests
{
    [Fact]
    public void ConsumerConfig_ComposesTheGroupAndTheDefaults()
    {
        ConsumerConfig config = new ConsumerConfiguration { BootstrapServers = "bus:9092", SaslUsername = "user", SaslPassword = "pass" }.ConsumerConfig("orders.handler");

        Assert.Equal("orders.handler", config.GroupId);
        Assert.Equal("bus:9092", config.BootstrapServers);
        Assert.True(config.EnableAutoCommit);
        Assert.False(config.EnableAutoOffsetStore);
        Assert.Equal(5_000, config.AutoCommitIntervalMs);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal(PartitionAssignmentStrategy.CooperativeSticky, config.PartitionAssignmentStrategy);
        Assert.Equal(Environment.MachineName, config.GroupInstanceId);
    }

    [Fact]
    public void ConsumerConfig_SuppliedValues_Win()
    {
        ConsumerConfig config = new ConsumerConfiguration
        {
            BootstrapServers = "bus:9092",
            SaslUsername = "user",
            SaslPassword = "pass",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest,
            GroupInstanceId = "instance-1"
        }.ConsumerConfig("orders.handler");

        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
        Assert.Equal("instance-1", config.GroupInstanceId);
    }
}
