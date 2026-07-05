using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// The global producer configuration, bound from the <c>Bus:Producer</c> section: the connection plus
/// the tuning overrides this bus supports (a curated surface, not every client knob). Unset values
/// fall back to <see cref="ProducerConfigurationDefaults"/> when composing the <see cref="ProducerConfig"/>.
/// </summary>
public sealed record ProducerConfiguration
{
    /// <summary>Comma-separated list of Kafka brokers. Required.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>SASL username. Required.</summary>
    public required string SaslUsername { get; init; }

    /// <summary>SASL password. Required.</summary>
    public required string SaslPassword { get; init; }

    /// <summary>Security protocol, or <see langword="null"/> for the default.</summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>SASL mechanism, or <see langword="null"/> for the default.</summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>Acknowledgment level, or <see langword="null"/> for the default.</summary>
    public Acks? Acks { get; init; }

    /// <summary>Auto-create topics, or <see langword="null"/> for the default.</summary>
    public bool? AllowAutoCreateTopics { get; init; }

    /// <summary>Idempotent delivery, or <see langword="null"/> for the default.</summary>
    public bool? EnableIdempotence { get; init; }

    /// <summary>Compression, or <see langword="null"/> for the default.</summary>
    public CompressionType? CompressionType { get; init; }

    /// <summary>Delivery timeout (ms), or <see langword="null"/> for the default.</summary>
    public int? MessageTimeoutMs { get; init; }

    /// <summary>Linger (ms), or <see langword="null"/> for the default.</summary>
    public double? LingerMs { get; init; }

    /// <summary>Messages per batch, or <see langword="null"/> for the default.</summary>
    public int? BatchNumMessages { get; init; }

    /// <summary>Batch size (bytes), or <see langword="null"/> for the default.</summary>
    public int? BatchSize { get; init; }

    /// <summary>Maximum size (bytes) of a single message, or <see langword="null"/> for the default.</summary>
    public int? MessageMaxBytes { get; init; }

    /// <summary>Max send retries, or <see langword="null"/> for the default.</summary>
    public int? MessageSendMaxRetries { get; init; }

    /// <summary>Retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMs { get; init; }

    /// <summary>Max retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMaxMs { get; init; }

    /// <summary>Client id, or <see langword="null"/> for the default (machine name).</summary>
    public string? ClientId { get; init; }

    /// <summary>Maximum messages in the producer's local queue — a full queue throws <c>Local_QueueFull</c> (the client's back-pressure signal) — or <see langword="null"/> for the client default (100000).</summary>
    public int? QueueBufferingMaxMessages { get; init; }

    /// <summary>Maximum kbytes in the producer's local queue (takes priority over the message count), or <see langword="null"/> for the client default (1048576).</summary>
    public int? QueueBufferingMaxKbytes { get; init; }

    /// <summary>Delivery report fields to marshal back (e.g. <c>none</c> when only the error is checked), or <see langword="null"/> for the client default (<c>all</c> — the setter rejects null, so the default is composed explicitly).</summary>
    public string? DeliveryReportFields { get; init; }

    /// <summary>Interval (ms) between statistics emissions (logged at Debug under the Kafka category), or <see langword="null"/> for none.</summary>
    public int? StatisticsIntervalMs { get; init; }

    /// <summary>librdkafka debug contexts (comma-separated, e.g. <c>broker,topic,msg</c>), or <see langword="null"/> for none.</summary>
    public string? Debug { get; init; }

    /// <summary>Whether broker disconnects are logged, or <see langword="null"/> for the client default (true) — the classic idle-connection noise.</summary>
    public bool? LogConnectionClose { get; init; }

    /// <summary>The Kafka producer configuration — supplied values, defaults for the rest.</summary>
    public ProducerConfig ProducerConfig => new()
    {
        BootstrapServers = BootstrapServers,
        SecurityProtocol = SecurityProtocol ?? ProducerConfigurationDefaults.SECURITY_PROTOCOL,
        SaslMechanism = SaslMechanism ?? ProducerConfigurationDefaults.SASL_MECHANISM,
        SaslUsername = SaslUsername,
        SaslPassword = SaslPassword,
        Acks = Acks ?? ProducerConfigurationDefaults.ACKS,
        AllowAutoCreateTopics = AllowAutoCreateTopics ?? ProducerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS,
        EnableIdempotence = EnableIdempotence ?? ProducerConfigurationDefaults.ENABLE_IDEMPOTENCE,
        CompressionType = CompressionType ?? ProducerConfigurationDefaults.COMPRESSION_TYPE,
        MessageTimeoutMs = MessageTimeoutMs ?? ProducerConfigurationDefaults.MESSAGE_TIMEOUT_MS,
        LingerMs = LingerMs ?? ProducerConfigurationDefaults.LINGER_MS,
        BatchNumMessages = BatchNumMessages ?? ProducerConfigurationDefaults.BATCH_NUM_MESSAGES,
        BatchSize = BatchSize ?? ProducerConfigurationDefaults.BATCH_SIZE,
        MessageMaxBytes = MessageMaxBytes ?? ProducerConfigurationDefaults.MESSAGE_MAX_BYTES,
        MessageSendMaxRetries = MessageSendMaxRetries ?? ProducerConfigurationDefaults.MESSAGE_SEND_MAX_RETRIES,
        RetryBackoffMs = RetryBackoffMs ?? ProducerConfigurationDefaults.RETRY_BACKOFF_MS,
        RetryBackoffMaxMs = RetryBackoffMaxMs ?? ProducerConfigurationDefaults.RETRY_BACKOFF_MAX_MS,
        ClientId = ClientId ?? ProducerConfigurationDefaults.CLIENT_ID,
        QueueBufferingMaxMessages = QueueBufferingMaxMessages,
        QueueBufferingMaxKbytes = QueueBufferingMaxKbytes,
        DeliveryReportFields = DeliveryReportFields ?? "all",
        StatisticsIntervalMs = StatisticsIntervalMs,
        Debug = Debug,
        LogConnectionClose = LogConnectionClose
    };
}
