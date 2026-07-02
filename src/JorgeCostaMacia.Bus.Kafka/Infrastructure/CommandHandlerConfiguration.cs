using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka consumer configuration for a command handler: its topic, group id, resilience policy
/// and the assembled <see cref="ConsumerConfig"/> (connection + consumer settings). The group id is
/// declared explicitly (e.g. <c>{topic}.handler</c>) — it is a contract holding the group's offsets
/// in the broker, so it must stay stable across refactors. The connection is supplied by the bus
/// builder (set once), so it is not repeated per handler by the developer.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type.</typeparam>
public sealed record CommandHandlerConfiguration<TCommand, TCommandHandler> : IHandlerConfiguration
    where TCommand : ICommand
    where TCommandHandler : ICommandHandler
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

    /// <inheritdoc />
    public Type MessageType { get; init; }

    /// <inheritdoc />
    public Type HandlerType { get; init; }

    /// <inheritdoc />
    public string Topic { get; init; }

    /// <inheritdoc />
    public string GroupId { get; init; }

    /// <inheritdoc />
    public int RetryAttempts { get; init; }

    /// <inheritdoc />
    public ImmutableList<Type> RetryExcludeExceptionTypes { get; init; }

    /// <inheritdoc />
    public int RedeliveryAttempts { get; init; }

    /// <inheritdoc />
    public ImmutableList<Type> RedeliveryExcludeExceptionTypes { get; init; }

    /// <summary>Configures the command handler; unsupplied consumer settings fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>) — a stable contract, it holds the group's offsets.</param>
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
    public CommandHandlerConfiguration(
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
        MessageType = typeof(TCommand);
        HandlerType = typeof(TCommandHandler);
        Topic = topic;
        RetryAttempts = retryAttempts ?? CommandHandlerConfigurationDefaults.RETRY_ATTEMPTS;
        RetryExcludeExceptionTypes = retryExcludeExceptionTypes ?? CommandHandlerConfigurationDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;
        RedeliveryAttempts = redeliveryAttempts ?? CommandHandlerConfigurationDefaults.REDELIVERY_ATTEMPTS;
        RedeliveryExcludeExceptionTypes = redeliveryExcludeExceptionTypes ?? CommandHandlerConfigurationDefaults.REDELIVERY_EXCLUDE_EXCEPTION_TYPES;

        _bootstrapServers = bootstrapServers;
        _saslUsername = saslUsername;
        _saslPassword = saslPassword;
        _securityProtocol = securityProtocol ?? CommandHandlerConfigurationDefaults.SECURITY_PROTOCOL;
        _saslMechanism = saslMechanism ?? CommandHandlerConfigurationDefaults.SASL_MECHANISM;
        _enableAutoCommit = enableAutoCommit ?? CommandHandlerConfigurationDefaults.ENABLE_AUTO_COMMIT;
        _enableAutoOffsetStore = enableAutoOffsetStore ?? CommandHandlerConfigurationDefaults.ENABLE_AUTO_OFFSET_STORE;
        _autoCommitIntervalMs = autoCommitIntervalMs ?? CommandHandlerConfigurationDefaults.AUTO_COMMIT_INTERVAL_MS;
        _allowAutoCreateTopics = allowAutoCreateTopics ?? CommandHandlerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS;
        _autoOffsetReset = autoOffsetReset ?? CommandHandlerConfigurationDefaults.AUTO_OFFSET_RESET;
        _socketTimeoutMs = socketTimeoutMs ?? CommandHandlerConfigurationDefaults.SOCKET_TIMEOUT_MS;
        _maxPollIntervalMs = maxPollIntervalMs ?? CommandHandlerConfigurationDefaults.MAX_POLL_INTERVAL_MS;
        _sessionTimeoutMs = sessionTimeoutMs ?? CommandHandlerConfigurationDefaults.SESSION_TIMEOUT_MS;
        _heartbeatIntervalMs = heartbeatIntervalMs ?? CommandHandlerConfigurationDefaults.HEARTBEAT_INTERVAL_MS;
        _retryBackoffMs = retryBackoffMs ?? CommandHandlerConfigurationDefaults.RETRY_BACKOFF_MS;
        _retryBackoffMaxMs = retryBackoffMaxMs ?? CommandHandlerConfigurationDefaults.RETRY_BACKOFF_MAX_MS;
        _clientId = clientId ?? CommandHandlerConfigurationDefaults.CLIENT_ID;
        GroupId = groupId;
        _groupInstanceId = groupInstanceId ?? CommandHandlerConfigurationDefaults.GROUP_INSTANCE_ID;
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
