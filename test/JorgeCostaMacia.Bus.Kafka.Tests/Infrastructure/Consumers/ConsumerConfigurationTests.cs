using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers;

public class ConsumerConfigurationTests
{
    [Fact]
    public void ConsumerConfig_ComposesTheGroupAndTheDefaults()
    {
        ConsumerConfig config = new ConsumerConfiguration
        {
            BootstrapServers = "bus:9092",
            SaslUsername = "user",
            SaslPassword = "pass"
        }.ConsumerConfig("orders.handler");

        Assert.Equal("orders.handler", config.GroupId);
        Assert.Equal("bus:9092", config.BootstrapServers);
        Assert.True(config.EnableAutoCommit);
        Assert.False(config.EnableAutoOffsetStore);
        Assert.Equal(5_000, config.AutoCommitIntervalMs);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal(PartitionAssignmentStrategy.CooperativeSticky, config.PartitionAssignmentStrategy);
        Assert.Equal(Environment.MachineName, config.GroupInstanceId);
        Assert.Equal(5_000, config.QueuedMinMessages);
        Assert.Equal(8_192, config.QueuedMaxMessagesKbytes);
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
            GroupInstanceId = "instance-1",
            QueuedMinMessages = 10_000,
            QueuedMaxMessagesKbytes = 16_384
        }.ConsumerConfig("orders.handler");

        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
        Assert.Equal("instance-1", config.GroupInstanceId);
        Assert.Equal(10_000, config.QueuedMinMessages);
        Assert.Equal(16_384, config.QueuedMaxMessagesKbytes);
    }
}
