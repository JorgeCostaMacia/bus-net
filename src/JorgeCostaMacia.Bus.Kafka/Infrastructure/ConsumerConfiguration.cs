using System.Collections.Immutable;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka consumer configuration for one handler or subscriber: its topic, group id, resilience
/// policy and the assembled <see cref="ConsumerConfig"/> (connection + consumer settings). The group
/// id is declared explicitly (e.g. <c>{topic}.handler</c> for a command handler,
/// <c>{consumer}.on.{topic}.subscriber</c> for an event subscriber) — it is a contract holding the
/// group's offsets in the broker, so it must stay stable across refactors. The connection is supplied
/// by the bus configurator (set once), so it is not repeated per handler by the developer.
/// </summary>
public sealed record ConsumerConfiguration
{
    private readonly string _bootstrapServers;
    private readonly SecurityProtocol _securityProtocol;
    private readonly SaslMechanism _saslMechanism;
    private readonly string? _saslUsername;
    private readonly string? _saslPassword;
    private readonly bool _enableAutoCommit;
    private readonly bool _enableAutoOffsetStore;
    private readonly int _autoCommitIntervalMs;
    private readonly bool _allowAutoCreateTopics;
    private readonly AutoOffsetReset _autoOffsetReset;
    private readonly int _socketTimeoutMs;
    private readonly int _maxPollIntervalMs;
    private readonly int _sessionTimeoutMs;
    private readonly int _heartbeatIntervalMs;
    private readonly int _retryBackoffMs;
    private readonly int _retryBackoffMaxMs;
    private readonly string _clientId;
    private readonly string _groupInstanceId;

    /// <summary>The Kafka topic the consumer subscribes to.</summary>
    public string Topic { get; init; }

    /// <summary>The consumer group id — the consumer's identity for offsets and consumer-side filtering.</summary>
    public string GroupId { get; init; }

    /// <summary>
    /// Maximum retry attempts when handling fails — each retry requeues the delivery to the topic's
    /// tail (0 means no retries).
    /// </summary>
    public int RetryAttempts { get; init; }

    /// <summary>Exception types excluded from retries.</summary>
    public ImmutableList<Type> RetryExcludeExceptionTypes { get; init; }

    /// <summary>Maximum redelivery attempts (re-queued by the bus) after failure.</summary>
    public int RedeliveryAttempts { get; init; }

    /// <summary>Exception types excluded from redelivery.</summary>
    public ImmutableList<Type> RedeliveryExcludeExceptionTypes { get; init; }

    /// <summary>Configures the consumer; unsupplied settings fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>, <c>{consumer}.on.{topic}.subscriber</c>) — a stable contract, it holds the group's offsets.</param>
    /// <param name="bootstrapServers">Comma-separated Kafka brokers.</param>
    /// <param name="saslUsername">SASL username, when authenticating.</param>
    /// <param name="saslPassword">SASL password, when authenticating.</param>
    /// <param name="retryAttempts">Maximum retry requeues to the topic, or <see langword="null"/> for the default (no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retries, or <see langword="null"/> for none.</param>
    /// <param name="redeliveryAttempts">Redelivery attempts, or <see langword="null"/> for the default.</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exceptions excluded from redelivery, or <see langword="null"/> for none.</param>
    /// <param name="securityProtocol">Security protocol, or <see langword="null"/> for the default.</param>
    /// <param name="saslMechanism">SASL mechanism, or <see langword="null"/> for the default.</param>
    /// <param name="enableAutoCommit">Background commit of stored offsets, or <see langword="null"/> for the default (true).</param>
    /// <param name="enableAutoOffsetStore">Store offsets automatically prior to delivery, or <see langword="null"/> for the default (false — the consumer stores after handling).</param>
    /// <param name="autoCommitIntervalMs">Interval (ms) between background commits of the stored offsets, or <see langword="null"/> for the default (5000).</param>
    /// <param name="allowAutoCreateTopics">Auto-create topics, or <see langword="null"/> for the default.</param>
    /// <param name="autoOffsetReset">Offset reset behavior, or <see langword="null"/> for the default.</param>
    /// <param name="socketTimeoutMs">Socket timeout (ms), or <see langword="null"/> for the default.</param>
    /// <param name="maxPollIntervalMs">Max poll interval (ms), or <see langword="null"/> for the default.</param>
    /// <param name="sessionTimeoutMs">Session timeout (ms), or <see langword="null"/> for the default.</param>
    /// <param name="heartbeatIntervalMs">Heartbeat interval (ms), or <see langword="null"/> for the default.</param>
    /// <param name="retryBackoffMs">Retry backoff (ms), or <see langword="null"/> for the default.</param>
    /// <param name="retryBackoffMaxMs">Max retry backoff (ms), or <see langword="null"/> for the default.</param>
    /// <param name="clientId">Client id, or <see langword="null"/> for the default (machine name).</param>
    /// <param name="groupInstanceId">Static group instance id, or <see langword="null"/> for the default (machine name).</param>
    public ConsumerConfiguration(
        string topic,
        string groupId,
        string bootstrapServers,
        string? saslUsername = null,
        string? saslPassword = null,
        int? retryAttempts = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        int? redeliveryAttempts = null,
        ImmutableList<Type>? redeliveryExcludeExceptionTypes = null,
        SecurityProtocol? securityProtocol = null,
        SaslMechanism? saslMechanism = null,
        bool? enableAutoCommit = null,
        bool? enableAutoOffsetStore = null,
        int? autoCommitIntervalMs = null,
        bool? allowAutoCreateTopics = null,
        AutoOffsetReset? autoOffsetReset = null,
        int? socketTimeoutMs = null,
        int? maxPollIntervalMs = null,
        int? sessionTimeoutMs = null,
        int? heartbeatIntervalMs = null,
        int? retryBackoffMs = null,
        int? retryBackoffMaxMs = null,
        string? clientId = null,
        string? groupInstanceId = null)
    {
        Topic = topic;
        GroupId = groupId;
        RetryAttempts = retryAttempts ?? ConsumerConfigurationDefaults.RETRY_ATTEMPTS;
        RetryExcludeExceptionTypes = retryExcludeExceptionTypes ?? ConsumerConfigurationDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;
        RedeliveryAttempts = redeliveryAttempts ?? ConsumerConfigurationDefaults.REDELIVERY_ATTEMPTS;
        RedeliveryExcludeExceptionTypes = redeliveryExcludeExceptionTypes ?? ConsumerConfigurationDefaults.REDELIVERY_EXCLUDE_EXCEPTION_TYPES;

        _bootstrapServers = bootstrapServers;
        _saslUsername = saslUsername;
        _saslPassword = saslPassword;
        _securityProtocol = securityProtocol ?? ConsumerConfigurationDefaults.SECURITY_PROTOCOL;
        _saslMechanism = saslMechanism ?? ConsumerConfigurationDefaults.SASL_MECHANISM;
        _enableAutoCommit = enableAutoCommit ?? ConsumerConfigurationDefaults.ENABLE_AUTO_COMMIT;
        _enableAutoOffsetStore = enableAutoOffsetStore ?? ConsumerConfigurationDefaults.ENABLE_AUTO_OFFSET_STORE;
        _autoCommitIntervalMs = autoCommitIntervalMs ?? ConsumerConfigurationDefaults.AUTO_COMMIT_INTERVAL_MS;
        _allowAutoCreateTopics = allowAutoCreateTopics ?? ConsumerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS;
        _autoOffsetReset = autoOffsetReset ?? ConsumerConfigurationDefaults.AUTO_OFFSET_RESET;
        _socketTimeoutMs = socketTimeoutMs ?? ConsumerConfigurationDefaults.SOCKET_TIMEOUT_MS;
        _maxPollIntervalMs = maxPollIntervalMs ?? ConsumerConfigurationDefaults.MAX_POLL_INTERVAL_MS;
        _sessionTimeoutMs = sessionTimeoutMs ?? ConsumerConfigurationDefaults.SESSION_TIMEOUT_MS;
        _heartbeatIntervalMs = heartbeatIntervalMs ?? ConsumerConfigurationDefaults.HEARTBEAT_INTERVAL_MS;
        _retryBackoffMs = retryBackoffMs ?? ConsumerConfigurationDefaults.RETRY_BACKOFF_MS;
        _retryBackoffMaxMs = retryBackoffMaxMs ?? ConsumerConfigurationDefaults.RETRY_BACKOFF_MAX_MS;
        _clientId = clientId ?? ConsumerConfigurationDefaults.CLIENT_ID;
        _groupInstanceId = groupInstanceId ?? ConsumerConfigurationDefaults.GROUP_INSTANCE_ID;
    }

    /// <summary>The Kafka consumer configuration assembled from the connection and settings.</summary>
    public ConsumerConfig ConsumerConfig => new()
    {
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = _securityProtocol,
        SaslMechanism = _saslMechanism,
        SaslUsername = _saslUsername,
        SaslPassword = _saslPassword,
        EnableAutoCommit = _enableAutoCommit,
        EnableAutoOffsetStore = _enableAutoOffsetStore,
        AutoCommitIntervalMs = _autoCommitIntervalMs,
        AllowAutoCreateTopics = _allowAutoCreateTopics,
        AutoOffsetReset = _autoOffsetReset,
        SocketTimeoutMs = _socketTimeoutMs,
        MaxPollIntervalMs = _maxPollIntervalMs,
        SessionTimeoutMs = _sessionTimeoutMs,
        HeartbeatIntervalMs = _heartbeatIntervalMs,
        RetryBackoffMs = _retryBackoffMs,
        RetryBackoffMaxMs = _retryBackoffMaxMs,
        ClientId = _clientId,
        GroupId = GroupId,
        GroupInstanceId = _groupInstanceId
    };
}
