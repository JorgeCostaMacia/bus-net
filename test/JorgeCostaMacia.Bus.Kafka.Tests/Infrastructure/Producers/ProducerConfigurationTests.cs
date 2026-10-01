using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Producers;

public class ProducerConfigurationTests
{
    [Fact]
    public void ProducerConfig_UnsetValues_FallBackToTheDefaults()
    {
        ProducerConfig config = new ProducerConfiguration { BootstrapServers = "bus:9092", SaslUsername = "user", SaslPassword = "pass" }.ProducerConfig;

        Assert.Equal("bus:9092", config.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(CompressionType.Lz4, config.CompressionType);
        Assert.Equal(50, config.LingerMs);
        Assert.Equal(int.MaxValue, config.MessageSendMaxRetries);
        Assert.Equal(120_000, config.MessageTimeoutMs);
        Assert.Equal(1_048_576, config.MessageMaxBytes);
        Assert.Equal(Environment.MachineName, config.ClientId);
        Assert.True(config.SocketKeepaliveEnable);
    }

    // A handler waits for its delivery and its consumer does not poll meanwhile: with the delivery
    // timeout at or above the poll interval, a broker slow to confirm evicts the consumer from its group
    // instead of failing the delivery into the bus's retry. Seen in production with both at 5 minutes.
    [Fact]
    public void Defaults_TheDeliveryTimeoutStaysBelowTheConsumersPollInterval()
        => Assert.True(ProducerConfigurationDefaults.MessageTimeoutMs < ConsumerConfigurationDefaults.MaxPollIntervalMs);

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
}
