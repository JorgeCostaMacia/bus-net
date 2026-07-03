using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The global consumer configuration, bound from the <c>Bus:Consumer</c> section: the connection plus
/// the tuning overrides this bus supports (a curated surface, not every client knob). Unset values
/// fall back to <see cref="ConsumerConfigurationDefaults"/> when composing each consumer's
/// <see cref="ConsumerConfig"/> — shared settings live here once; what varies per consumer (the group
/// id) is supplied when composing.
/// </summary>
public sealed class ConsumerConfiguration
{
    /// <summary>Comma-separated list of Kafka brokers. Required when the service consumes.</summary>
    public string? BootstrapServers { get; init; }

    /// <summary>SASL username, when authenticating.</summary>
    public string? SaslUsername { get; init; }

    /// <summary>SASL password, when authenticating.</summary>
    public string? SaslPassword { get; init; }

    /// <summary>Security protocol, or <see langword="null"/> for the default.</summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>SASL mechanism, or <see langword="null"/> for the default.</summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>Background commit of stored offsets, or <see langword="null"/> for the default (true).</summary>
    public bool? EnableAutoCommit { get; init; }

    /// <summary>Store offsets automatically prior to delivery, or <see langword="null"/> for the default (false — the consumer stores after handling).</summary>
    public bool? EnableAutoOffsetStore { get; init; }

    /// <summary>Interval (ms) between background commits of the stored offsets, or <see langword="null"/> for the default (5000).</summary>
    public int? AutoCommitIntervalMs { get; init; }

    /// <summary>Auto-create topics, or <see langword="null"/> for the default.</summary>
    public bool? AllowAutoCreateTopics { get; init; }

    /// <summary>Offset reset behavior, or <see langword="null"/> for the default.</summary>
    public AutoOffsetReset? AutoOffsetReset { get; init; }

    /// <summary>Socket timeout (ms), or <see langword="null"/> for the default.</summary>
    public int? SocketTimeoutMs { get; init; }

    /// <summary>Max poll interval (ms), or <see langword="null"/> for the default.</summary>
    public int? MaxPollIntervalMs { get; init; }

    /// <summary>Session timeout (ms), or <see langword="null"/> for the default.</summary>
    public int? SessionTimeoutMs { get; init; }

    /// <summary>Heartbeat interval (ms), or <see langword="null"/> for the default.</summary>
    public int? HeartbeatIntervalMs { get; init; }

    /// <summary>Retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMs { get; init; }

    /// <summary>Max retry backoff (ms), or <see langword="null"/> for the default.</summary>
    public int? RetryBackoffMaxMs { get; init; }

    /// <summary>Client id, or <see langword="null"/> for the default (machine name).</summary>
    public string? ClientId { get; init; }

    /// <summary>Static group instance id, or <see langword="null"/> for the default (machine name).</summary>
    public string? GroupInstanceId { get; init; }

    /// <summary>
    /// Composes the Kafka consumer configuration for one consumer — supplied values, defaults for the
    /// rest, and the consumer's own group id.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <returns>The assembled consumer configuration.</returns>
    /// <exception cref="InvalidOperationException"><c>Bus:Consumer:BootstrapServers</c> is missing.</exception>
    public ConsumerConfig ConsumerConfig(string groupId)
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new InvalidOperationException($"'Bus:Consumer:{nameof(BootstrapServers)}' is null.");
        }

        return new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = SecurityProtocol ?? ConsumerConfigurationDefaults.SECURITY_PROTOCOL,
            SaslMechanism = SaslMechanism ?? ConsumerConfigurationDefaults.SASL_MECHANISM,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
            EnableAutoCommit = EnableAutoCommit ?? ConsumerConfigurationDefaults.ENABLE_AUTO_COMMIT,
            EnableAutoOffsetStore = EnableAutoOffsetStore ?? ConsumerConfigurationDefaults.ENABLE_AUTO_OFFSET_STORE,
            AutoCommitIntervalMs = AutoCommitIntervalMs ?? ConsumerConfigurationDefaults.AUTO_COMMIT_INTERVAL_MS,
            AllowAutoCreateTopics = AllowAutoCreateTopics ?? ConsumerConfigurationDefaults.ALLOW_AUTO_CREATE_TOPICS,
            AutoOffsetReset = AutoOffsetReset ?? ConsumerConfigurationDefaults.AUTO_OFFSET_RESET,
            SocketTimeoutMs = SocketTimeoutMs ?? ConsumerConfigurationDefaults.SOCKET_TIMEOUT_MS,
            MaxPollIntervalMs = MaxPollIntervalMs ?? ConsumerConfigurationDefaults.MAX_POLL_INTERVAL_MS,
            SessionTimeoutMs = SessionTimeoutMs ?? ConsumerConfigurationDefaults.SESSION_TIMEOUT_MS,
            HeartbeatIntervalMs = HeartbeatIntervalMs ?? ConsumerConfigurationDefaults.HEARTBEAT_INTERVAL_MS,
            RetryBackoffMs = RetryBackoffMs ?? ConsumerConfigurationDefaults.RETRY_BACKOFF_MS,
            RetryBackoffMaxMs = RetryBackoffMaxMs ?? ConsumerConfigurationDefaults.RETRY_BACKOFF_MAX_MS,
            ClientId = ClientId ?? ConsumerConfigurationDefaults.CLIENT_ID,
            GroupId = groupId,
            GroupInstanceId = GroupInstanceId ?? ConsumerConfigurationDefaults.GROUP_INSTANCE_ID
        };
    }
}
