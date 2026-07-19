using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// Default producer settings a <see cref="ProducerConfiguration"/> falls back to for values the
/// <c>Bus:Producer</c> section does not supply. Shared by every message; topics themselves are
/// infrastructure, auto-created by the broker with its defaults and managed broker-side.
/// </summary>
public static class ProducerConfigurationDefaults
{
    /// <summary>Security protocol used to communicate with the brokers. Default: <c>SaslSsl</c> — SASL authentication over a TLS transport.</summary>
    public const SecurityProtocol SecurityProtocol = SecurityProtocol.SaslSsl;

    /// <summary>SASL authentication mechanism. Default: <c>ScramSha512</c>.</summary>
    public const SaslMechanism SaslMechanism = SaslMechanism.ScramSha512;

    /// <summary>Producer acknowledgment level. Default: <c>All</c>.</summary>
    public const Acks Acks = Acks.All;

    /// <summary>
    /// Whether topics can be auto-created if missing. Default: <c>true</c> — topics are born on first
    /// use with the broker's defaults (partitions/replication/min-isr) and managed broker-side.
    /// </summary>
    public const bool AllowAutoCreateTopics = true;

    /// <summary>Enables idempotent message delivery. Default: <c>true</c>.</summary>
    public const bool EnableIdempotence = true;

    /// <summary>Compression type for producer messages. Default: <c>Lz4</c> — fast, the de-facto modern default; applied per batch over the serialized byte payloads (effective on text-like data, near-neutral on already-compressed bytes).</summary>
    public const CompressionType CompressionType = CompressionType.Lz4;

    /// <summary>Maximum time (ms) to wait for message delivery. Default: <c>300000</c> (5 min).</summary>
    public const int MessageTimeoutMs = 300_000;

    /// <summary>Producer linger time (ms) before sending a batch. Default: <c>50</c>.</summary>
    public const double LingerMs = 50;

    /// <summary>Maximum number of messages per batch. Default: <c>10000</c>.</summary>
    public const int BatchNumMessages = 10_000;

    /// <summary>Batch size in bytes. Default: <c>1000000</c> (1 MB).</summary>
    public const int BatchSize = 1_000_000;

    /// <summary>
    /// Maximum size (bytes) of a single message/request. Default: <c>1048576</c> (1 MB) — the standard
    /// Kafka size, matching the broker's default <c>message.max.bytes</c> (and the consumer's default
    /// fetch), so no broker-side change is needed. Messages are expected to be small: structured domain
    /// data, where even a text invoice PDF in Base64 is ~5–80 KB. A genuinely large payload (a scanned or
    /// image blob) must NOT travel the bus — use the claim-check pattern: store the blob externally and
    /// send a small reference instead.
    /// </summary>
    public const int MessageMaxBytes = 1_048_576;

    /// <summary>
    /// Maximum number of retries on send failure. Default: <see cref="int.MaxValue"/> — retries are
    /// bounded by time (<see cref="MessageTimeoutMs"/>, 5 min), not by a count, which is the
    /// idiomatic choice with <see cref="EnableIdempotence"/> on (order and no-duplicates preserved
    /// across retries). The back-off below is what keeps a struggling broker from being hammered.
    /// </summary>
    public const int MessageSendMaxRetries = int.MaxValue;

    /// <summary>Base backoff (ms) between retries. Default: <c>500</c>.</summary>
    public const int RetryBackoffMs = 500;

    /// <summary>Maximum backoff (ms) between retries. Default: <c>10000</c> (10 s).</summary>
    public const int RetryBackoffMaxMs = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string ClientId => Environment.MachineName;

    /// <summary>
    /// Whether TCP keepalive is enabled on broker connections. Default: <c>true</c> — the producer is a
    /// long-lived singleton, and keepalive stops idle connections from being silently dropped by
    /// firewalls/NAT (which would otherwise surface as spurious reconnects).
    /// </summary>
    public const bool SocketKeepaliveEnable = true;

    /// <summary>Delivery report fields marshalled back on each result. Default: <c>all</c> (the client rejects <see langword="null"/>, so the default is set explicitly).</summary>
    public const string DeliveryReportFields = "all";
}
