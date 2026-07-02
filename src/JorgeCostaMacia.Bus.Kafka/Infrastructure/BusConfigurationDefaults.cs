using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default global (connection + producer tuning) settings applied to a <see cref="BusConfiguration"/>
/// when not supplied. Shared by every message; topics themselves are infrastructure, auto-created by
/// the broker with its defaults and managed broker-side.
/// </summary>
public static class BusConfigurationDefaults
{
    /// <summary>Security protocol used to communicate with the brokers. Default: <c>Ssl</c>.</summary>
    public const SecurityProtocol SECURITY_PROTOCOL = SecurityProtocol.Ssl;

    /// <summary>SASL authentication mechanism. Default: <c>ScramSha512</c>.</summary>
    public const SaslMechanism SASL_MECHANISM = SaslMechanism.ScramSha512;

    /// <summary>Producer acknowledgment level. Default: <c>All</c>.</summary>
    public const Acks ACKS = Acks.All;

    /// <summary>
    /// Whether topics can be auto-created if missing. Default: <c>true</c> — topics are born on first
    /// use with the broker's defaults (partitions/replication/min-isr) and managed broker-side.
    /// </summary>
    public const bool ALLOW_AUTO_CREATE_TOPICS = true;

    /// <summary>Enables idempotent message delivery. Default: <c>true</c>.</summary>
    public const bool ENABLE_IDEMPOTENCE = true;

    /// <summary>Compression type for producer messages. Default: <c>Snappy</c>.</summary>
    public const CompressionType COMPRESSION_TYPE = CompressionType.Snappy;

    /// <summary>Maximum time (ms) to wait for message delivery. Default: <c>300000</c> (5 min).</summary>
    public const int MESSAGE_TIMEOUT_MS = 300_000;

    /// <summary>Producer linger time (ms) before sending a batch. Default: <c>50</c>.</summary>
    public const double LINGER_MS = 50;

    /// <summary>Maximum number of messages per batch. Default: <c>100</c>.</summary>
    public const int BATCH_NUM_MESSAGES = 100;

    /// <summary>Batch size in bytes. Default: <c>2097152</c> (2 MB).</summary>
    public const int BATCH_SIZE = 2_097_152;

    /// <summary>Maximum number of retries on send failure. Default: <c>10</c>.</summary>
    public const int MESSAGE_SEND_MAX_RETRIES = 10;

    /// <summary>Base backoff (ms) between retries. Default: <c>500</c>.</summary>
    public const int RETRY_BACKOFF_MS = 500;

    /// <summary>Maximum backoff (ms) between retries. Default: <c>10000</c> (10 s).</summary>
    public const int RETRY_BACKOFF_MAX_MS = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string CLIENT_ID => Environment.MachineName;
}
