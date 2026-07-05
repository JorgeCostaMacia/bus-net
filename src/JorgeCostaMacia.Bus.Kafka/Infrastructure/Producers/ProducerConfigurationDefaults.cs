using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// Default producer settings a <see cref="ProducerConfiguration"/> falls back to for values the
/// <c>Bus:Producer</c> section does not supply. Shared by every message; topics themselves are
/// infrastructure, auto-created by the broker with its defaults and managed broker-side.
/// </summary>
public static class ProducerConfigurationDefaults
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

    /// <summary>Compression type for producer messages. Default: <c>Lz4</c> — fast, good ratio on JSON, the de-facto modern default; applied per batch.</summary>
    public const CompressionType COMPRESSION_TYPE = CompressionType.Lz4;

    /// <summary>Maximum time (ms) to wait for message delivery. Default: <c>300000</c> (5 min).</summary>
    public const int MESSAGE_TIMEOUT_MS = 300_000;

    /// <summary>Producer linger time (ms) before sending a batch. Default: <c>50</c>.</summary>
    public const double LINGER_MS = 50;

    /// <summary>Maximum number of messages per batch. Default: <c>10000</c>.</summary>
    public const int BATCH_NUM_MESSAGES = 10_000;

    /// <summary>Batch size in bytes. Default: <c>1000000</c> (1 MB).</summary>
    public const int BATCH_SIZE = 1_000_000;

    /// <summary>
    /// Maximum size (bytes) of a single message/request. Default: <c>2097152</c> (2 MB) — headroom
    /// above the per-batch size for the occasional message carrying a small file (e.g. a 1–2 page PDF
    /// in Base64). The broker's <c>message.max.bytes</c> and the topic's <c>max.message.bytes</c> must
    /// allow at least this, and consumers must fetch at least this (<c>max.partition.fetch.bytes</c>).
    /// </summary>
    public const int MESSAGE_MAX_BYTES = 2_097_152;

    /// <summary>
    /// Maximum number of retries on send failure. Default: <see cref="int.MaxValue"/> — retries are
    /// bounded by time (<see cref="MESSAGE_TIMEOUT_MS"/>, 5 min), not by a count, which is the
    /// idiomatic choice with <see cref="ENABLE_IDEMPOTENCE"/> on (order and no-duplicates preserved
    /// across retries). The back-off below is what keeps a struggling broker from being hammered.
    /// </summary>
    public const int MESSAGE_SEND_MAX_RETRIES = int.MaxValue;

    /// <summary>Base backoff (ms) between retries. Default: <c>500</c>.</summary>
    public const int RETRY_BACKOFF_MS = 500;

    /// <summary>Maximum backoff (ms) between retries. Default: <c>10000</c> (10 s).</summary>
    public const int RETRY_BACKOFF_MAX_MS = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string CLIENT_ID => Environment.MachineName;
}
