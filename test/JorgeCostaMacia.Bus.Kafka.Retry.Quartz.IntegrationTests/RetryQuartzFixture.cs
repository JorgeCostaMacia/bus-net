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

    /// <summary>Starts both containers, pulling the images on first use, then provisions the Quartz schema.</summary>
    public async ValueTask InitializeAsync()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await Task.WhenAll(
            _kafka.StartAsync(cancellationToken),
            _postgres.StartAsync(cancellationToken));

        await CreateQuartzSchema(cancellationToken);
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
        await using NpgsqlConnection connection = new(PostgresConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using NpgsqlCommand command = new("SELECT count(*) FROM qrtz_job_details", connection);

        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// Creates the Quartz ADO tables in the Postgres container with the store's default <c>qrtz_</c>
    /// table prefix in the default <c>public</c> schema — the canonical Quartz PostgreSQL DDL, which
    /// the persistent store queries once it starts.
    /// </summary>
    private async Task CreateQuartzSchema(CancellationToken cancellationToken)
    {
        await using NpgsqlConnection connection = new(PostgresConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using NpgsqlCommand command = new(QuartzSchema, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    // The canonical Quartz.NET PostgreSQL schema (tables_postgres.sql): default 'qrtz_' table prefix,
    // default 'public' schema. Quartz issues unquoted, upper-cased identifiers ('QRTZ_') that Postgres
    // folds to lower case, so the store's default configuration matches these table names as created.
    private const string QuartzSchema = """
        CREATE TABLE qrtz_job_details (sched_name TEXT NOT NULL, job_name TEXT NOT NULL, job_group TEXT NOT NULL, description TEXT NULL, job_class_name TEXT NOT NULL, is_durable BOOL NOT NULL, is_nonconcurrent BOOL NOT NULL, is_update_data BOOL NOT NULL, requests_recovery BOOL NOT NULL, job_data BYTEA NULL, PRIMARY KEY (sched_name, job_name, job_group));
        CREATE TABLE qrtz_triggers (sched_name TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, job_name TEXT NOT NULL, job_group TEXT NOT NULL, description TEXT NULL, next_fire_time BIGINT NULL, prev_fire_time BIGINT NULL, priority INTEGER NULL, trigger_state TEXT NOT NULL, trigger_type TEXT NOT NULL, start_time BIGINT NOT NULL, end_time BIGINT NULL, calendar_name TEXT NULL, misfire_instr SMALLINT NULL, misfire_orig_fire_time BIGINT NULL, execution_group VARCHAR(200) NULL, job_data BYTEA NULL, PRIMARY KEY (sched_name, trigger_name, trigger_group), FOREIGN KEY (sched_name, job_name, job_group) REFERENCES qrtz_job_details (sched_name, job_name, job_group));
        CREATE TABLE qrtz_simple_triggers (sched_name TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, repeat_count BIGINT NOT NULL, repeat_interval BIGINT NOT NULL, times_triggered BIGINT NOT NULL, PRIMARY KEY (sched_name, trigger_name, trigger_group), FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES qrtz_triggers (sched_name, trigger_name, trigger_group) ON DELETE CASCADE);
        CREATE TABLE qrtz_simprop_triggers (sched_name TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, str_prop_1 TEXT NULL, str_prop_2 TEXT NULL, str_prop_3 TEXT NULL, int_prop_1 INTEGER NULL, int_prop_2 INTEGER NULL, long_prop_1 BIGINT NULL, long_prop_2 BIGINT NULL, dec_prop_1 NUMERIC NULL, dec_prop_2 NUMERIC NULL, bool_prop_1 BOOL NULL, bool_prop_2 BOOL NULL, time_zone_id TEXT NULL, PRIMARY KEY (sched_name, trigger_name, trigger_group), FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES qrtz_triggers (sched_name, trigger_name, trigger_group) ON DELETE CASCADE);
        CREATE TABLE qrtz_cron_triggers (sched_name TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, cron_expression TEXT NOT NULL, time_zone_id TEXT, PRIMARY KEY (sched_name, trigger_name, trigger_group), FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES qrtz_triggers (sched_name, trigger_name, trigger_group) ON DELETE CASCADE);
        CREATE TABLE qrtz_blob_triggers (sched_name TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, blob_data BYTEA NULL, PRIMARY KEY (sched_name, trigger_name, trigger_group), FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES qrtz_triggers (sched_name, trigger_name, trigger_group) ON DELETE CASCADE);
        CREATE TABLE qrtz_calendars (sched_name TEXT NOT NULL, calendar_name TEXT NOT NULL, calendar BYTEA NOT NULL, PRIMARY KEY (sched_name, calendar_name));
        CREATE TABLE qrtz_paused_trigger_grps (sched_name TEXT NOT NULL, trigger_group TEXT NOT NULL, PRIMARY KEY (sched_name, trigger_group));
        CREATE TABLE qrtz_fired_triggers (sched_name TEXT NOT NULL, entry_id TEXT NOT NULL, trigger_name TEXT NOT NULL, trigger_group TEXT NOT NULL, instance_name TEXT NOT NULL, fired_time BIGINT NOT NULL, sched_time BIGINT NOT NULL, priority INTEGER NOT NULL, state TEXT NOT NULL, job_name TEXT NULL, job_group TEXT NULL, is_nonconcurrent BOOL NOT NULL, requests_recovery BOOL NULL, execution_group VARCHAR(200) NULL, PRIMARY KEY (sched_name, entry_id));
        CREATE TABLE qrtz_scheduler_state (sched_name TEXT NOT NULL, instance_name TEXT NOT NULL, last_checkin_time BIGINT NOT NULL, checkin_interval BIGINT NOT NULL, PRIMARY KEY (sched_name, instance_name));
        CREATE TABLE qrtz_locks (sched_name TEXT NOT NULL, lock_name TEXT NOT NULL, PRIMARY KEY (sched_name, lock_name));
        CREATE INDEX idx_qrtz_j_req_recovery ON qrtz_job_details (requests_recovery);
        CREATE INDEX idx_qrtz_t_next_fire_time ON qrtz_triggers (next_fire_time);
        CREATE INDEX idx_qrtz_t_state ON qrtz_triggers (trigger_state);
        CREATE INDEX idx_qrtz_t_nft_st ON qrtz_triggers (next_fire_time, trigger_state);
        CREATE INDEX idx_qrtz_ft_trig_name ON qrtz_fired_triggers (trigger_name);
        CREATE INDEX idx_qrtz_ft_trig_group ON qrtz_fired_triggers (trigger_group);
        CREATE INDEX idx_qrtz_ft_trig_nm_gp ON qrtz_fired_triggers (sched_name, trigger_name, trigger_group);
        CREATE INDEX idx_qrtz_ft_trig_inst_name ON qrtz_fired_triggers (instance_name);
        CREATE INDEX idx_qrtz_ft_job_name ON qrtz_fired_triggers (job_name);
        CREATE INDEX idx_qrtz_ft_job_group ON qrtz_fired_triggers (job_group);
        CREATE INDEX idx_qrtz_ft_job_req_recovery ON qrtz_fired_triggers (requests_recovery);
        """;
}
