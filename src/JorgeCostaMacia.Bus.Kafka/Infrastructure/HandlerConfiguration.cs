using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The per-handler custom configuration — the bus's own policy, apart from Kafka's: the topic, the
/// group id and the resilience policy. The group id is declared explicitly (e.g.
/// <c>{topic}.handler</c> for a command handler, <c>{consumer}.on.{topic}.subscriber</c> for an event
/// subscriber) — it is a contract holding the group's offsets in the broker, so it must stay stable
/// across refactors.
/// </summary>
public sealed record HandlerConfiguration
{
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

    /// <summary>Configures the handler's custom policy; unsupplied resilience settings fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>, <c>{consumer}.on.{topic}.subscriber</c>) — a stable contract, it holds the group's offsets.</param>
    /// <param name="retryAttempts">Maximum retry requeues to the topic, or <see langword="null"/> for the default (no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retries, or <see langword="null"/> for none.</param>
    /// <param name="redeliveryAttempts">Redelivery attempts, or <see langword="null"/> for the default.</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exceptions excluded from redelivery, or <see langword="null"/> for none.</param>
    public HandlerConfiguration(
        string topic,
        string groupId,
        int? retryAttempts = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        int? redeliveryAttempts = null,
        ImmutableList<Type>? redeliveryExcludeExceptionTypes = null)
    {
        Topic = topic;
        GroupId = groupId;
        RetryAttempts = retryAttempts ?? HandlerConfigurationDefaults.RETRY_ATTEMPTS;
        RetryExcludeExceptionTypes = retryExcludeExceptionTypes ?? HandlerConfigurationDefaults.RETRY_EXCLUDE_EXCEPTION_TYPES;
        RedeliveryAttempts = redeliveryAttempts ?? HandlerConfigurationDefaults.REDELIVERY_ATTEMPTS;
        RedeliveryExcludeExceptionTypes = redeliveryExcludeExceptionTypes ?? HandlerConfigurationDefaults.REDELIVERY_EXCLUDE_EXCEPTION_TYPES;
    }
}
