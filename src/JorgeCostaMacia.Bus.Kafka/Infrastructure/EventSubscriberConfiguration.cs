using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka consumer configuration for an event subscriber: its topic, concurrency, resilience
/// policy and the assembled <see cref="ConsumerConfig"/> (connection + consumer settings). The group
/// id is derived as <c>{topic}.subscriber</c>. The connection is supplied by the bus builder (set
/// once), so it is not repeated per subscriber by the developer.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type.</typeparam>
public sealed record EventSubscriberConfiguration<TEvent, TEventSubscriber> : IHandlerConfiguration
    where TEvent : IEvent
    where TEventSubscriber : IEventSubscriber
{
    private readonly string _bootstrapServers;
    private readonly SecurityProtocol _securityProtocol;
    private readonly SaslMechanism _saslMechanism;
    private readonly string _saslUsername;
    private readonly string _saslPassword;
    private readonly bool _enableAutoCommit;
    private readonly bool _allowAutoCreateTopics;
    private readonly AutoOffsetReset _autoOffsetReset;
    private readonly int _socketTimeoutMs;
    private readonly int _maxPollIntervalMs;
    private readonly int _sessionTimeoutMs;
    private readonly int _heartbeatIntervalMs;
    private readonly int _retryBackoffMs;
    private readonly int _retryBackoffMaxMs;
    private readonly string _clientId;
    private readonly string _groupId;
    private readonly string _groupInstanceId;

    /// <inheritdoc />
    public Type MessageType { get; init; }

    /// <inheritdoc />
    public Type HandlerType { get; init; }

    /// <inheritdoc />
    public string Topic { get; init; }

    /// <inheritdoc />
    public int Consumers { get; init; }

    /// <inheritdoc />
    public int RetryAttempts { get; init; }

    /// <inheritdoc />
    public ImmutableList<Type> RetryAttemptsExcludeExceptionTypes { get; init; }

    /// <inheritdoc />
    public int RedeliveryAttempts { get; init; }

    /// <inheritdoc />
    public ImmutableList<Type> RedeliveryExcludeExceptionTypes { get; init; }

    /// <summary>Configures the event subscriber; unsupplied consumer settings fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic to consume from (group id is derived as <c>{topic}.subscriber</c>).</param>
    /// <param name="bootstrapServers">Comma-separated Kafka brokers.</param>
    /// <param name="saslUsername">SASL username, when authenticating.</param>
    /// <param name="saslPassword">SASL password, when authenticating.</param>
    /// <param name="consumers">Concurrent consumer instances, or <see langword="null"/> for the default.</param>
    /// <param name="retryAttempts">In-process retry attempts, or <see langword="null"/> for the default.</param>
    /// <param name="retryAttemptsExcludeExceptionTypes">Exceptions excluded from retries, or <see langword="null"/> for none.</param>
    /// <param name="redeliveryAttempts">Redelivery attempts, or <see langword="null"/> for the default.</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exceptions excluded from redelivery, or <see langword="null"/> for none.</param>
    /// <param name="securityProtocol">Security protocol, or <see langword="null"/> for the default.</param>
    /// <param name="saslMechanism">SASL mechanism, or <see langword="null"/> for the default.</param>
    /// <param name="enableAutoCommit">Auto-commit offsets, or <see langword="null"/> for the default (false).</param>
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
    public EventSubscriberConfiguration(
        string topic,
        string bootstrapServers,
        string saslUsername,
        string saslPassword,
        int? consumers = null,
        int? retryAttempts = null,
        ImmutableList<Type>? retryAttemptsExcludeExceptionTypes = null,
        int? redeliveryAttempts = null,
        ImmutableList<Type>? redeliveryExcludeExceptionTypes = null,
        SecurityProtocol? securityProtocol = null,
        SaslMechanism? saslMechanism = null,
        bool? enableAutoCommit = null,
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
        MessageType = typeof(TEvent);
        HandlerType = typeof(TEventSubscriber);
        Topic = topic;
        Consumers = consumers ?? HandlerConfigurationDefaults.CONSUMERS;
        RetryAttempts = retryAttempts ?? HandlerConfigurationDefaults.RETRY_ATTEMPTS;
        RetryAttemptsExcludeExceptionTypes = retryAttemptsExcludeExceptionTypes ?? HandlerConfigurationDefaults.RETRY_ATTEMPTS_EXCLUDE_EXCEPTION_TYPES;
        RedeliveryAttempts = redeliveryAttempts ?? HandlerConfigurationDefaults.REDELIVERY_ATTEMPTS;
        RedeliveryExcludeExceptionTypes = redeliveryExcludeExceptionTypes ?? HandlerConfigurationDefaults.REDELIVERY_EXCLUDE_EXCEPTION_TYPES;

        _bootstrapServers = bootstrapServers;
        _saslUsername = saslUsername;
        _saslPassword = saslPassword;
        _securityProtocol = securityProtocol ?? HandlerConfigurationDefaults.SECURITY_PROTOCOL;
        _saslMechanism = saslMechanism ?? HandlerConfigurationDefaults.SASL_MECHANISM;
        _enableAutoCommit = enableAutoCommit ?? HandlerConfigurationDefaults.ENABLE_AUTO_COMMIT;
        _allowAutoCreateTopics = allowAutoCreateTopics ?? HandlerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS;
        _autoOffsetReset = autoOffsetReset ?? HandlerConfigurationDefaults.AUTO_OFFSET_RESET;
        _socketTimeoutMs = socketTimeoutMs ?? HandlerConfigurationDefaults.SOCKET_TIMEOUT_MS;
        _maxPollIntervalMs = maxPollIntervalMs ?? HandlerConfigurationDefaults.MAX_POLL_INTERVAL_MS;
        _sessionTimeoutMs = sessionTimeoutMs ?? HandlerConfigurationDefaults.SESSION_TIMEOUT_MS;
        _heartbeatIntervalMs = heartbeatIntervalMs ?? HandlerConfigurationDefaults.HEARTBEAT_INTERVAL_MS;
        _retryBackoffMs = retryBackoffMs ?? HandlerConfigurationDefaults.RETRY_BACKOFF_MS;
        _retryBackoffMaxMs = retryBackoffMaxMs ?? HandlerConfigurationDefaults.RETRY_BACKOFF_MAX_MS;
        _clientId = clientId ?? HandlerConfigurationDefaults.CLIENT_ID;
        _groupId = $"{topic}.subscriber";
        _groupInstanceId = groupInstanceId ?? HandlerConfigurationDefaults.GROUP_INSTANCE_ID;
    }

    /// <inheritdoc />
    public ConsumerConfig ConsumerConfig => new()
    {
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = _securityProtocol,
        SaslMechanism = _saslMechanism,
        SaslUsername = _saslUsername,
        SaslPassword = _saslPassword,
        EnableAutoCommit = _enableAutoCommit,
        AllowAutoCreateTopics = _allowAutoCreateTopics,
        AutoOffsetReset = _autoOffsetReset,
        SocketTimeoutMs = _socketTimeoutMs,
        MaxPollIntervalMs = _maxPollIntervalMs,
        SessionTimeoutMs = _sessionTimeoutMs,
        HeartbeatIntervalMs = _heartbeatIntervalMs,
        RetryBackoffMs = _retryBackoffMs,
        RetryBackoffMaxMs = _retryBackoffMaxMs,
        ClientId = _clientId,
        GroupId = _groupId,
        GroupInstanceId = _groupInstanceId
    };
}
