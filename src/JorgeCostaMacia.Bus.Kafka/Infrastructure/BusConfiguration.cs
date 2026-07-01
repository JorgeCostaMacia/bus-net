using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The global Kafka configuration: connection + producer tuning, shared by every message. Composes
/// the <see cref="ProducerConfig"/> and <see cref="AdminClientConfig"/> from the supplied connection
/// details and tuning overrides, falling back to <see cref="BusConfigurationDefaults"/>.
/// </summary>
public sealed class BusConfiguration : IBusConfiguration
{
    private readonly string _bootstrapServers;
    private readonly string? _saslUsername;
    private readonly string? _saslPassword;
    private readonly SecurityProtocol _securityProtocol;
    private readonly SaslMechanism _saslMechanism;
    private readonly Acks _acks;
    private readonly bool _allowAutoCreateTopics;
    private readonly bool _enableIdempotence;
    private readonly CompressionType _compressionType;
    private readonly int _messageTimeoutMs;
    private readonly double _lingerMs;
    private readonly int _batchNumMessages;
    private readonly int _batchSize;
    private readonly int _messageSendMaxRetries;
    private readonly int _retryBackoffMs;
    private readonly int _retryBackoffMaxMs;
    private readonly string _clientId;

    /// <summary>Builds the global configuration; tuning overrides fall back to the defaults.</summary>
    /// <param name="bootstrapServers">Comma-separated list of Kafka brokers.</param>
    /// <param name="saslUsername">SASL username, when authenticating.</param>
    /// <param name="saslPassword">SASL password, when authenticating.</param>
    /// <param name="securityProtocol">Security protocol, or <see langword="null"/> for the default.</param>
    /// <param name="saslMechanism">SASL mechanism, or <see langword="null"/> for the default.</param>
    /// <param name="acks">Acknowledgment level, or <see langword="null"/> for the default.</param>
    /// <param name="allowAutoCreateTopics">Auto-create topics, or <see langword="null"/> for the default.</param>
    /// <param name="enableIdempotence">Idempotent delivery, or <see langword="null"/> for the default.</param>
    /// <param name="compressionType">Compression, or <see langword="null"/> for the default.</param>
    /// <param name="messageTimeoutMs">Delivery timeout (ms), or <see langword="null"/> for the default.</param>
    /// <param name="lingerMs">Linger (ms), or <see langword="null"/> for the default.</param>
    /// <param name="batchNumMessages">Messages per batch, or <see langword="null"/> for the default.</param>
    /// <param name="batchSize">Batch size (bytes), or <see langword="null"/> for the default.</param>
    /// <param name="messageSendMaxRetries">Max send retries, or <see langword="null"/> for the default.</param>
    /// <param name="retryBackoffMs">Retry backoff (ms), or <see langword="null"/> for the default.</param>
    /// <param name="retryBackoffMaxMs">Max retry backoff (ms), or <see langword="null"/> for the default.</param>
    /// <param name="clientId">Client id, or <see langword="null"/> for the default (machine name).</param>
    public BusConfiguration(
        string bootstrapServers,
        string? saslUsername = null,
        string? saslPassword = null,
        SecurityProtocol? securityProtocol = null,
        SaslMechanism? saslMechanism = null,
        Acks? acks = null,
        bool? allowAutoCreateTopics = null,
        bool? enableIdempotence = null,
        CompressionType? compressionType = null,
        int? messageTimeoutMs = null,
        double? lingerMs = null,
        int? batchNumMessages = null,
        int? batchSize = null,
        int? messageSendMaxRetries = null,
        int? retryBackoffMs = null,
        int? retryBackoffMaxMs = null,
        string? clientId = null)
    {
        _bootstrapServers = bootstrapServers;
        _saslUsername = saslUsername;
        _saslPassword = saslPassword;
        _securityProtocol = securityProtocol ?? BusConfigurationDefaults.SECURITY_PROTOCOL;
        _saslMechanism = saslMechanism ?? BusConfigurationDefaults.SASL_MECHANISM;
        _acks = acks ?? BusConfigurationDefaults.ACKS;
        _allowAutoCreateTopics = allowAutoCreateTopics ?? BusConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS;
        _enableIdempotence = enableIdempotence ?? BusConfigurationDefaults.ENABLE_IDEMPOTENCE;
        _compressionType = compressionType ?? BusConfigurationDefaults.COMPRESSION_TYPE;
        _messageTimeoutMs = messageTimeoutMs ?? BusConfigurationDefaults.MESSAGE_TIMEOUT_MS;
        _lingerMs = lingerMs ?? BusConfigurationDefaults.LINGER_MS;
        _batchNumMessages = batchNumMessages ?? BusConfigurationDefaults.BATCH_NUM_MESSAGES;
        _batchSize = batchSize ?? BusConfigurationDefaults.BATCH_SIZE;
        _messageSendMaxRetries = messageSendMaxRetries ?? BusConfigurationDefaults.MESSAGE_SEND_MAX_RETRIES;
        _retryBackoffMs = retryBackoffMs ?? BusConfigurationDefaults.RETRY_BACKOFF_MS;
        _retryBackoffMaxMs = retryBackoffMaxMs ?? BusConfigurationDefaults.RETRY_BACKOFF_MAX_MS;
        _clientId = clientId ?? BusConfigurationDefaults.CLIENT_ID;
    }

    /// <inheritdoc />
    public ProducerConfig ProducerConfig => new()
    {
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = _securityProtocol,
        SaslMechanism = _saslMechanism,
        SaslUsername = _saslUsername,
        SaslPassword = _saslPassword,
        Acks = _acks,
        AllowAutoCreateTopics = _allowAutoCreateTopics,
        EnableIdempotence = _enableIdempotence,
        CompressionType = _compressionType,
        MessageTimeoutMs = _messageTimeoutMs,
        LingerMs = _lingerMs,
        BatchNumMessages = _batchNumMessages,
        BatchSize = _batchSize,
        MessageSendMaxRetries = _messageSendMaxRetries,
        RetryBackoffMs = _retryBackoffMs,
        RetryBackoffMaxMs = _retryBackoffMaxMs,
        ClientId = _clientId
    };

    /// <inheritdoc />
    public AdminClientConfig AdminClientConfig => new()
    {
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = _securityProtocol,
        SaslMechanism = _saslMechanism,
        SaslUsername = _saslUsername,
        SaslPassword = _saslPassword,
        ClientId = _clientId
    };
}
