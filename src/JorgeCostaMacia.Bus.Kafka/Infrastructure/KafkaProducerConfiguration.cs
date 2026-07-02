using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The global producer configuration, bound from the <c>Bus:Producer</c> section: the connection plus
/// the tuning overrides this bus supports (a curated surface, not every client knob). Unset values
/// fall back to <see cref="KafkaProducerConfigurationDefaults"/> when composing the <see cref="ProducerConfig"/>.
/// </summary>
public sealed record KafkaProducerConfiguration
{
    private const string SECTION = "Bus:Producer";

    /// <summary>Maps the <c>Bus:Producer</c> section onto a <see cref="KafkaProducerConfiguration"/>.</summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global producer configuration.</returns>
    /// <exception cref="InvalidOperationException"><c>Bus:Producer:BootstrapServers</c> is missing.</exception>
    internal static KafkaProducerConfiguration Create(IConfiguration configuration)
    {
        KafkaProducerConfiguration producer = configuration.GetSection(SECTION).Get<KafkaProducerConfiguration>() ?? new KafkaProducerConfiguration();

        if (string.IsNullOrWhiteSpace(producer.BootstrapServers))
        {
            throw new InvalidOperationException($"'{SECTION}:{nameof(BootstrapServers)}' is null.");
        }

        return producer;
    }

    /// <summary>Comma-separated list of Kafka brokers. Required.</summary>
    public string? BootstrapServers { get; init; }

    /// <summary>SASL username, when authenticating.</summary>
    public string? SaslUsername { get; init; }

    /// <summary>SASL password, when authenticating.</summary>
    public string? SaslPassword { get; init; }

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

    /// <summary>Max send retries, or <see langword="null"/> for the default.</summary>
    public int? MessageSendMaxRetries { get; init; }

    /// <summary>Retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMs { get; init; }

    /// <summary>Max retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMaxMs { get; init; }

    /// <summary>Client id, or <see langword="null"/> for the default (machine name).</summary>
    public string? ClientId { get; init; }

    /// <summary>The Kafka producer configuration — supplied values, defaults for the rest.</summary>
    public ProducerConfig ProducerConfig => new()
    {
        BootstrapServers = BootstrapServers,
        SecurityProtocol = SecurityProtocol ?? KafkaProducerConfigurationDefaults.SECURITY_PROTOCOL,
        SaslMechanism = SaslMechanism ?? KafkaProducerConfigurationDefaults.SASL_MECHANISM,
        SaslUsername = SaslUsername,
        SaslPassword = SaslPassword,
        Acks = Acks ?? KafkaProducerConfigurationDefaults.ACKS,
        AllowAutoCreateTopics = AllowAutoCreateTopics ?? KafkaProducerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS,
        EnableIdempotence = EnableIdempotence ?? KafkaProducerConfigurationDefaults.ENABLE_IDEMPOTENCE,
        CompressionType = CompressionType ?? KafkaProducerConfigurationDefaults.COMPRESSION_TYPE,
        MessageTimeoutMs = MessageTimeoutMs ?? KafkaProducerConfigurationDefaults.MESSAGE_TIMEOUT_MS,
        LingerMs = LingerMs ?? KafkaProducerConfigurationDefaults.LINGER_MS,
        BatchNumMessages = BatchNumMessages ?? KafkaProducerConfigurationDefaults.BATCH_NUM_MESSAGES,
        BatchSize = BatchSize ?? KafkaProducerConfigurationDefaults.BATCH_SIZE,
        MessageSendMaxRetries = MessageSendMaxRetries ?? KafkaProducerConfigurationDefaults.MESSAGE_SEND_MAX_RETRIES,
        RetryBackoffMs = RetryBackoffMs ?? KafkaProducerConfigurationDefaults.RETRY_BACKOFF_MS,
        RetryBackoffMaxMs = RetryBackoffMaxMs ?? KafkaProducerConfigurationDefaults.RETRY_BACKOFF_MAX_MS,
        ClientId = ClientId ?? KafkaProducerConfigurationDefaults.CLIENT_ID
    };
}
