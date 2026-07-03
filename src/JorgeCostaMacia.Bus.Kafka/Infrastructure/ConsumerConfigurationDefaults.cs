using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default Kafka consumer settings a <see cref="ConsumerConfiguration"/> falls back to for
/// values the <c>Bus:Consumer</c> section does not supply.
/// </summary>
public static class ConsumerConfigurationDefaults
{
    /// <summary>Security protocol. Default: <c>Ssl</c>.</summary>
    public const SecurityProtocol SECURITY_PROTOCOL = SecurityProtocol.Ssl;

    /// <summary>SASL mechanism. Default: <c>ScramSha512</c>.</summary>
    public const SaslMechanism SASL_MECHANISM = SaslMechanism.ScramSha512;

    /// <summary>
    /// Whether the background thread periodically commits the stored offsets. Default: <c>true</c> —
    /// combined with <see cref="ENABLE_AUTO_OFFSET_STORE"/> this is the store-offsets pattern: the
    /// consumer stores the offset only after handling, and the commit happens without blocking.
    /// </summary>
    public const bool ENABLE_AUTO_COMMIT = true;

    /// <summary>
    /// Whether offsets are stored automatically prior to delivery. Default: <c>false</c> — the
    /// consumer stores the offset explicitly after the message is handled (the store is the ack).
    /// </summary>
    public const bool ENABLE_AUTO_OFFSET_STORE = false;

    /// <summary>
    /// Interval (ms) at which the background thread commits the stored offsets — bounds the
    /// retry window after a hard crash (throughput × interval). Default: <c>5000</c>.
    /// </summary>
    public const int AUTO_COMMIT_INTERVAL_MS = 5_000;

    /// <summary>
    /// Whether topics can be auto-created on subscribe. Default: <c>true</c> — topics are born on
    /// first use with the broker's defaults (partitions/replication/min-isr) and managed broker-side.
    /// </summary>
    public const bool ALLOW_AUTO_CREATE_TOPICS = true;

    /// <summary>Where to start when no offset is stored. Default: <c>Latest</c>.</summary>
    public const AutoOffsetReset AUTO_OFFSET_RESET = AutoOffsetReset.Latest;

    /// <summary>
    /// Partition assignment strategy. Default: <c>CooperativeSticky</c> — incremental rebalancing:
    /// scaling instances in or out moves only the partitions that must move, instead of the eager
    /// stop-the-world revoke of the whole group.
    /// </summary>
    public const PartitionAssignmentStrategy PARTITION_ASSIGNMENT_STRATEGY = PartitionAssignmentStrategy.CooperativeSticky;

    /// <summary>Socket timeout (ms). Default: <c>90000</c>.</summary>
    public const int SOCKET_TIMEOUT_MS = 90_000;

    /// <summary>Max interval between polls before the consumer is considered failed (ms). Default: <c>300000</c>.</summary>
    public const int MAX_POLL_INTERVAL_MS = 300_000;

    /// <summary>Consumer session timeout (ms). Default: <c>45000</c>.</summary>
    public const int SESSION_TIMEOUT_MS = 45_000;

    /// <summary>Heartbeat interval (ms). Default: <c>10000</c>.</summary>
    public const int HEARTBEAT_INTERVAL_MS = 10_000;

    /// <summary>Base retry backoff (ms). Default: <c>500</c>.</summary>
    public const int RETRY_BACKOFF_MS = 500;

    /// <summary>Maximum retry backoff (ms). Default: <c>10000</c>.</summary>
    public const int RETRY_BACKOFF_MAX_MS = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string CLIENT_ID => Environment.MachineName;

    /// <summary>
    /// Static consumer group instance id. Default: <see cref="Environment.MachineName"/> (unique per
    /// container) — a restart within the session timeout keeps the partition assignment with no
    /// rebalance; note a static member does not leave the group on close (eviction is by session
    /// timeout).
    /// </summary>
    public static string GROUP_INSTANCE_ID => Environment.MachineName;
}
