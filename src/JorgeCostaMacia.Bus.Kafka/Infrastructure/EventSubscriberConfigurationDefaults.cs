using System.Collections.Immutable;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default consumer settings applied to an <see cref="EventSubscriberConfiguration{TEvent, TEventSubscriber}"/>
/// when not supplied.
/// </summary>
public static class EventSubscriberConfigurationDefaults
{
    /// <summary>Delays between in-process retry attempts. Default: empty (no retries).</summary>
    public static ImmutableList<TimeSpan> RETRY_INTERVALS => [];

    /// <summary>Exception types excluded from retries. Default: empty.</summary>
    public static ImmutableList<Type> RETRY_EXCLUDE_EXCEPTION_TYPES => [];

    /// <summary>Maximum redelivery attempts. Default: <c>0</c>.</summary>
    public const int REDELIVERY_ATTEMPTS = 0;

    /// <summary>Exception types excluded from redelivery. Default: empty.</summary>
    public static ImmutableList<Type> REDELIVERY_EXCLUDE_EXCEPTION_TYPES => [];

    /// <summary>Security protocol. Default: <c>Ssl</c>.</summary>
    public const SecurityProtocol SECURITY_PROTOCOL = SecurityProtocol.Ssl;

    /// <summary>SASL mechanism. Default: <c>ScramSha512</c>.</summary>
    public const SaslMechanism SASL_MECHANISM = SaslMechanism.ScramSha512;

    /// <summary>Whether the consumer auto-commits offsets. Default: <c>false</c> (manual commit as ack).</summary>
    public const bool ENABLE_AUTO_COMMIT = false;

    /// <summary>Whether topics can be auto-created on subscribe. Default: <c>false</c>.</summary>
    public const bool ALLOW_AUTO_CREATE_TOPICS = false;

    /// <summary>Where to start when no offset is stored. Default: <c>Latest</c>.</summary>
    public static AutoOffsetReset AUTO_OFFSET_RESET => AutoOffsetReset.Latest;

    /// <summary>Socket timeout (ms). Default: <c>90000</c>.</summary>
    public const int SOCKET_TIMEOUT_MS = 90_000;

    /// <summary>Max interval between polls before the consumer is considered failed (ms). Default: <c>300000</c>.</summary>
    public const int MAX_POLL_INTERVAL_MS = 300_000;

    /// <summary>Consumer session timeout (ms). Default: <c>45000</c>.</summary>
    public const int SESSION_TIMEOUT_MS = 45_000;

    /// <summary>Heartbeat interval (ms). Default: <c>10000</c>.</summary>
    public const int HEARTBEAT_INTERVAL_MS = 10_000;

    /// <summary>Base retry backoff (ms). Default: <c>500</c>.</summary>
    public const int RETRY_BACKOFF_MS = 500;

    /// <summary>Maximum retry backoff (ms). Default: <c>10000</c>.</summary>
    public const int RETRY_BACKOFF_MAX_MS = 10_000;

    /// <summary>Kafka client identifier. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string CLIENT_ID => Environment.MachineName;

    /// <summary>Static consumer group instance id. Default: <see cref="Environment.MachineName"/>.</summary>
    public static string GROUP_INSTANCE_ID => Environment.MachineName;
}
