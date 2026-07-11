using Microsoft.Extensions.Configuration;
using Testcontainers.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// The shared Kafka broker for the integration suite: boots one ephemeral, single-broker
/// <see cref="KafkaContainer"/> from a pinned image, exposes its bootstrap endpoint, and disposes it
/// (stop + remove) when the fixture tears down. Shared across a test class as an
/// <see cref="IClassFixture{TFixture}"/> so the broker starts once, not once per test.
/// </summary>
public sealed class KafkaFixture : IAsyncLifetime
{
    // The image the Testcontainers.Kafka 4.13.0 module is built against; the module bakes the
    // advertised-listener wiring for it, so pin exactly this rather than a floating tag.
    private const string KafkaImage = "confluentinc/cp-kafka:7.5.12";

    private readonly KafkaContainer _container = new KafkaBuilder(KafkaImage)
        .Build();

    /// <summary>Starts the container, pulling the image on first use.</summary>
    public ValueTask InitializeAsync()
        => new(_container.StartAsync(TestContext.Current.CancellationToken));

    /// <summary>Stops and removes the container.</summary>
    public ValueTask DisposeAsync()
        => _container.DisposeAsync();

    /// <summary>
    /// Builds the bus configuration for the running container's <c>Bus:Producer</c> and
    /// <c>Bus:Consumer</c> sections: the mapped bootstrap endpoint with <c>SecurityProtocol</c> forced
    /// to <c>Plaintext</c> — the container speaks plain, unauthenticated Kafka while the bus defaults to
    /// <c>SaslSsl</c> + SCRAM, so the integration config must downgrade the protocol and point at the
    /// mapped bootstrap address. The <c>SaslUsername</c>/<c>SaslPassword</c> are dummy values present
    /// only to satisfy the bus's required-field validation; under <c>Plaintext</c> librdkafka never
    /// sends them (SASL is inert), so their contents are irrelevant.
    /// </summary>
    /// <returns>An in-memory configuration carrying the <c>Bus:Producer</c> and <c>Bus:Consumer</c> keys.</returns>
    public IConfiguration BuildConfiguration()
    {
        // GetBootstrapAddress() returns a UriBuilder string (PLAINTEXT://host:port); librdkafka's
        // bootstrap.servers wants a bare host:port list, so take the authority.
        string bootstrapServers = new Uri(_container.GetBootstrapAddress()).Authority;

        Dictionary<string, string?> settings = new()
        {
            ["Bus:Producer:BootstrapServers"] = bootstrapServers,
            ["Bus:Producer:SecurityProtocol"] = "Plaintext",
            ["Bus:Producer:SaslUsername"] = "test",
            ["Bus:Producer:SaslPassword"] = "test",
            ["Bus:Consumer:BootstrapServers"] = bootstrapServers,
            ["Bus:Consumer:SecurityProtocol"] = "Plaintext",
            ["Bus:Consumer:SaslUsername"] = "test",
            ["Bus:Consumer:SaslPassword"] = "test"
        };

        return new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();
    }
}
