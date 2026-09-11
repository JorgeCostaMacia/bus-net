using Microsoft.Extensions.Configuration;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.IntegrationTests;

/// <summary>
/// The shared infrastructure for the scheduled-retry suite: boots one ephemeral, single-broker
/// <see cref="KafkaContainer"/> and one ephemeral <see cref="PostgreSqlContainer"/> (both from pinned
/// images), creates the Quartz ADO tables in Postgres so the persistent store has its schema, and
/// disposes both when the fixture tears down. Shared across the test class as an
/// <see cref="IClassFixture{TFixture}"/> so the pair starts once, not once per test.
/// </summary>
public sealed class RetryQuartzFixture : IAsyncLifetime
{
    // The image the Testcontainers.Kafka 4.13.0 module is built against; the module bakes the
    // advertised-listener wiring for it, so pin exactly this rather than a floating tag.
    private const string KafkaImage = "confluentinc/cp-kafka:7.5.12";
    private const string PostgresImage = "postgres:16";

    private readonly KafkaContainer _kafka = new KafkaBuilder(KafkaImage)
        .Build();

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder(PostgresImage)
        .Build();

    /// <summary>The Npgsql connection string of the running Postgres container — the Quartz store points here.</summary>
    public string PostgresConnectionString => _postgres.GetConnectionString();

    /// <summary>Starts both containers, pulling the images on first use. The Quartz schema is provisioned by the store itself — see the test's <c>ProvisionSchema</c>.</summary>
    public async ValueTask InitializeAsync()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await Task.WhenAll(
            _kafka.StartAsync(cancellationToken),
            _postgres.StartAsync(cancellationToken));
    }

    /// <summary>Stops and removes both containers.</summary>
    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }

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
        string bootstrapServers = new Uri(_kafka.GetBootstrapAddress()).Authority;

        Dictionary<string, string?> settings = new Dictionary<string, string?>()
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

    /// <summary>Counts the durable jobs parked in the Quartz store — a positive count is a retry parked in Postgres.</summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The number of rows in <c>qrtz_job_details</c>.</returns>
    public async Task<long> CountParkedJobs(CancellationToken cancellationToken)
    {
        await using NpgsqlConnection connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using NpgsqlCommand command = new NpgsqlCommand("SELECT count(*) FROM qrtz_job_details", connection);

        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
