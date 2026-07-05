using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ConfigurationTests
{
    [Fact]
    public void ProducerConfig_UnsetValues_FallBackToTheDefaults()
    {
        ProducerConfig config = new ProducerConfiguration { BootstrapServers = "bus:9092", SaslUsername = "user", SaslPassword = "pass" }.ProducerConfig;

        Assert.Equal("bus:9092", config.BootstrapServers);
        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(CompressionType.Lz4, config.CompressionType);
        Assert.Equal(50, config.LingerMs);
        Assert.Equal(int.MaxValue, config.MessageSendMaxRetries);
        Assert.Equal(2_097_152, config.MessageMaxBytes);
        Assert.Equal(Environment.MachineName, config.ClientId);
    }

    [Fact]
    public void ProducerConfig_SuppliedValues_Win()
    {
        ProducerConfig config = new ProducerConfiguration
        {
            BootstrapServers = "bus:9092",
            SaslUsername = "user",
            SaslPassword = "pass",
            SecurityProtocol = SecurityProtocol.Plaintext,
            Acks = Acks.Leader,
            CompressionType = CompressionType.Gzip,
            ClientId = "custom"
        }.ProducerConfig;

        Assert.Equal(SecurityProtocol.Plaintext, config.SecurityProtocol);
        Assert.Equal(Acks.Leader, config.Acks);
        Assert.Equal(CompressionType.Gzip, config.CompressionType);
        Assert.Equal("custom", config.ClientId);
    }

    [Fact]
    public void ConsumerConfig_ComposesTheGroupAndTheDefaults()
    {
        ConsumerConfig config = new ConsumerConfiguration { BootstrapServers = "bus:9092", SaslUsername = "user", SaslPassword = "pass" }.ConsumerConfig("orders.handler");

        Assert.Equal("orders.handler", config.GroupId);
        Assert.Equal("bus:9092", config.BootstrapServers);
        Assert.True(config.EnableAutoCommit);
        Assert.False(config.EnableAutoOffsetStore);
        Assert.Equal(5_000, config.AutoCommitIntervalMs);
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
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
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupInstanceId = "instance-1"
        }.ConsumerConfig("orders.handler");

        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal("instance-1", config.GroupInstanceId);
    }
}
