using System.Collections.Immutable;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Per-handler Kafka consumer configuration: the message type it consumes, the handler type, its
/// topic and concurrency, the resilience policy (retry / redelivery) and the assembled
/// <see cref="ConsumerConfig"/> (connection + consumer settings, including the group id).
/// Held by the bus, keyed by message type.
/// </summary>
public interface IHandlerConfiguration
{
    /// <summary>The CLR type of the message this handler consumes.</summary>
    Type MessageType { get; }

    /// <summary>The CLR type of the handler.</summary>
    Type HandlerType { get; }

    /// <summary>The Kafka topic the handler subscribes to.</summary>
    string Topic { get; }

    /// <summary>The consumer group id — the handler's identity for offsets and consumer-side filtering.</summary>
    string GroupId { get; }

    /// <summary>
    /// Maximum retry attempts when handling fails — each retry requeues the delivery to the topic's
    /// tail, targeted to this consumer only (0 means no retries).
    /// </summary>
    int RetryAttempts { get; }

    /// <summary>Exception types excluded from retries.</summary>
    ImmutableList<Type> RetryExcludeExceptionTypes { get; }

    /// <summary>Maximum redelivery attempts (re-queued by the bus) after failure.</summary>
    int RedeliveryAttempts { get; }

    /// <summary>Exception types excluded from redelivery.</summary>
    ImmutableList<Type> RedeliveryExcludeExceptionTypes { get; }

    /// <summary>The assembled Kafka consumer configuration (connection + consumer settings).</summary>
    ConsumerConfig ConsumerConfig { get; }
}
