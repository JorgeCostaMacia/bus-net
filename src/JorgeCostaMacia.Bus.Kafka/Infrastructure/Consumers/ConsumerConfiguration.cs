using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

/// <summary>
/// The global consumer configuration, bound from the <c>Bus:Consumer</c> section: the connection plus
/// the tuning overrides this bus supports (a curated surface, not every client knob). Unset values
/// fall back to <see cref="ConsumerConfigurationDefaults"/> when composing each consumer's
/// <see cref="ConsumerConfig"/> — shared settings live here once; what varies per consumer (the group
/// id) is supplied when composing.
/// </summary>
public sealed record ConsumerConfiguration
{
    /// <summary>Comma-separated list of Kafka brokers. Required when the service consumes.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>SASL username. Required when the service consumes.</summary>
    public required string SaslUsername { get; init; }

    /// <summary>SASL password. Required when the service consumes.</summary>
    public required string SaslPassword { get; init; }

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

    /// <summary>Partition assignment strategy, or <see langword="null"/> for the default (CooperativeSticky — incremental rebalancing).</summary>
    public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; init; }

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

    /// <summary>Minimum messages the client prefetches per partition, or <see langword="null"/> for the client default (100000).</summary>
    public int? QueuedMinMessages { get; init; }

    /// <summary>Maximum kbytes the client prefetches per partition — dominates the consumer's memory footprint when a process hosts many workers — or <see langword="null"/> for the client default (65536).</summary>
    public int? QueuedMaxMessagesKbytes { get; init; }

    /// <summary>Interval (ms) between statistics emissions (logged at Debug under the Kafka category), or <see langword="null"/> for none.</summary>
    public int? StatisticsIntervalMs { get; init; }

    /// <summary>librdkafka debug contexts (comma-separated, e.g. <c>consumer,cgrp,topic,fetch</c>), or <see langword="null"/> for none.</summary>
    public string? Debug { get; init; }

    /// <summary>Whether broker disconnects are logged, or <see langword="null"/> for the client default (true) — the classic idle-connection noise.</summary>
    public bool? LogConnectionClose { get; init; }

    /// <summary>
    /// Maximum number of this service's consumers opening their initial broker connection at once at
    /// startup, or <see langword="null"/> for the default (8). Not a Kafka client setting — it bounds
    /// the startup handshake surge (see <see cref="StartupGate"/>); raise it on a cluster that absorbs
    /// more concurrent connects.
    /// </summary>
    public int? StartupMaxConcurrency { get; init; }

    /// <summary>
    /// Composes the Kafka consumer configuration for one consumer — supplied values, defaults for the
    /// rest, and the consumer's own group id.
    /// </summary>
    /// <param name="groupId">The consumer group id.</param>
    /// <returns>The assembled consumer configuration.</returns>
    public ConsumerConfig ConsumerConfig(string groupId)
        => new ConsumerConfig()
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = SecurityProtocol ?? ConsumerConfigurationDefaults.SecurityProtocol,
            SaslMechanism = SaslMechanism ?? ConsumerConfigurationDefaults.SaslMechanism,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword,
            EnableAutoCommit = EnableAutoCommit ?? ConsumerConfigurationDefaults.EnableAutoCommit,
            EnableAutoOffsetStore = EnableAutoOffsetStore ?? ConsumerConfigurationDefaults.EnableAutoOffsetStore,
            AutoCommitIntervalMs = AutoCommitIntervalMs ?? ConsumerConfigurationDefaults.AutoCommitIntervalMs,
            AllowAutoCreateTopics = AllowAutoCreateTopics ?? ConsumerConfigurationDefaults.AllowAutoCreateTopics,
            AutoOffsetReset = AutoOffsetReset ?? ConsumerConfigurationDefaults.AutoOffsetReset,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy ?? ConsumerConfigurationDefaults.PartitionAssignmentStrategy,
            SocketTimeoutMs = SocketTimeoutMs ?? ConsumerConfigurationDefaults.SocketTimeoutMs,
            MaxPollIntervalMs = MaxPollIntervalMs ?? ConsumerConfigurationDefaults.MaxPollIntervalMs,
            SessionTimeoutMs = SessionTimeoutMs ?? ConsumerConfigurationDefaults.SessionTimeoutMs,
            HeartbeatIntervalMs = HeartbeatIntervalMs ?? ConsumerConfigurationDefaults.HeartbeatIntervalMs,
            RetryBackoffMs = RetryBackoffMs ?? ConsumerConfigurationDefaults.RetryBackoffMs,
            RetryBackoffMaxMs = RetryBackoffMaxMs ?? ConsumerConfigurationDefaults.RetryBackoffMaxMs,
            ClientId = ClientId ?? ConsumerConfigurationDefaults.ClientId,
            GroupId = groupId,
            GroupInstanceId = GroupInstanceId ?? ConsumerConfigurationDefaults.GroupInstanceId,
            QueuedMinMessages = QueuedMinMessages,
            QueuedMaxMessagesKbytes = QueuedMaxMessagesKbytes,
            StatisticsIntervalMs = StatisticsIntervalMs,
            Debug = Debug,
            LogConnectionClose = LogConnectionClose
        };
}
