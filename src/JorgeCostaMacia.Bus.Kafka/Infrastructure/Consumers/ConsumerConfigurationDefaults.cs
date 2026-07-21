using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

/// <summary>
/// Default Kafka consumer settings a <see cref="ConsumerConfiguration"/> falls back to for
/// values the <c>Bus:Consumer</c> section does not supply.
/// </summary>
public static class ConsumerConfigurationDefaults
{
    /// <summary>Security protocol. Default: <c>SaslSsl</c> — the SASL credentials this configuration requires are only sent under a SASL protocol (and it matches the producer default).</summary>
    public const SecurityProtocol SecurityProtocol = SecurityProtocol.SaslSsl;

    /// <summary>SASL mechanism. Default: <c>ScramSha512</c>.</summary>
    public const SaslMechanism SaslMechanism = SaslMechanism.ScramSha512;

    /// <summary>
    /// Whether the background thread periodically commits the stored offsets. Default: <c>true</c> —
    /// combined with <see cref="EnableAutoOffsetStore"/> this is the store-offsets pattern: the
    /// consumer stores the offset only after handling, and the commit happens without blocking.
    /// </summary>
    public const bool EnableAutoCommit = true;

    /// <summary>
    /// Whether offsets are stored automatically prior to delivery. Default: <c>false</c> — the
    /// consumer stores the offset explicitly after the message is handled (the store is the ack).
    /// </summary>
    public const bool EnableAutoOffsetStore = false;

    /// <summary>
    /// Interval (ms) at which the background thread commits the stored offsets — bounds the
    /// retry window after a hard crash (throughput × interval). Default: <c>5000</c>.
    /// </summary>
    public const int AutoCommitIntervalMs = 5_000;

    /// <summary>
    /// Whether topics can be auto-created on subscribe. Default: <c>true</c> — topics are born on
    /// first use with the broker's defaults (partitions/replication/min-isr) and managed broker-side.
    /// </summary>
    public const bool AllowAutoCreateTopics = true;

    /// <summary>
    /// Where to start when no offset is stored (a group's very first start, or expired offsets).
    /// Default: <c>Earliest</c> — the at-least-once bias: a duplicate is absorbed by the idempotent
    /// handling, a silently skipped message is invisible loss; and it matches the RabbitMQ queue
    /// semantics, where everything published since the queue existed waits for its consumer.
    /// </summary>
    public const AutoOffsetReset AutoOffsetReset = AutoOffsetReset.Earliest;

    /// <summary>
    /// Partition assignment strategy. Default: <c>CooperativeSticky</c> — incremental rebalancing:
    /// scaling instances in or out moves only the partitions that must move, instead of the eager
    /// stop-the-world revoke of the whole group.
    /// </summary>
    public const PartitionAssignmentStrategy PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;

    /// <summary>Socket timeout (ms). Default: <c>90000</c>.</summary>
    public const int SocketTimeoutMs = 90_000;

    /// <summary>Max interval between polls before the consumer is considered failed (ms). Default: <c>300000</c>.</summary>
    public const int MaxPollIntervalMs = 300_000;

    /// <summary>Consumer session timeout (ms). Default: <c>45000</c>.</summary>
    public const int SessionTimeoutMs = 45_000;

    /// <summary>Heartbeat interval (ms). Default: <c>10000</c>.</summary>
    public const int HeartbeatIntervalMs = 10_000;

    /// <summary>Base retry backoff (ms). Default: <c>500</c>.</summary>
    public const int RetryBackoffMs = 500;

    /// <summary>Maximum retry backoff (ms). Default: <c>10000</c>.</summary>
    public const int RetryBackoffMaxMs = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string ClientId => Environment.MachineName;

    /// <summary>
    /// Static consumer group instance id. Default: <see cref="Environment.MachineName"/> (unique per
    /// container) — a restart within the session timeout keeps the partition assignment with no
    /// rebalance; note a static member does not leave the group on close (eviction is by session
    /// timeout).
    /// </summary>
    public static string GroupInstanceId => Environment.MachineName;

    /// <summary>
    /// Maximum consumers opening their initial broker connection at once at startup. Default: <c>8</c> —
    /// staggers the startup handshakes (see <see cref="StartupGate"/>) so a service with many consumers
    /// does not connect them all in the same instant.
    /// </summary>
    public const int StartupMaxConcurrency = 8;
}
