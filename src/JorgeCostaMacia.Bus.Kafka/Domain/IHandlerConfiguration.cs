using System.Collections.Immutable;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Per-handler Kafka consumer configuration: the message type it consumes, the handler type, its
/// topic and concurrency, the resilience policy (retry / redelivery) and the assembled
/// <see cref="ConsumerConfig"/> (connection + consumer settings, including the derived group id).
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

    /// <summary>Number of concurrent consumer instances for this handler.</summary>
    int Consumers { get; }

    /// <summary>
    /// Delays between in-process retry attempts when handling fails — one entry per attempt, waited
    /// before it (empty means no retries).
    /// </summary>
    ImmutableList<TimeSpan> RetryIntervals { get; }

    /// <summary>Exception types excluded from retries.</summary>
    ImmutableList<Type> RetryExcludeExceptionTypes { get; }

    /// <summary>Maximum redelivery attempts (re-queued by the bus) after failure.</summary>
    int RedeliveryAttempts { get; }

    /// <summary>Exception types excluded from redelivery.</summary>
    ImmutableList<Type> RedeliveryExcludeExceptionTypes { get; }

    /// <summary>The assembled Kafka consumer configuration (connection + consumer settings).</summary>
    ConsumerConfig ConsumerConfig { get; }

    /// <summary>Handler for consumer errors (connection-level and fatal), or <see langword="null"/> for none.</summary>
    Action<IConsumer<Null, byte[]>, Error>? ErrorHandler { get; }

    /// <summary>Handler for the consumer's internal (librdkafka) log messages, or <see langword="null"/> for none.</summary>
    Action<IConsumer<Null, byte[]>, LogMessage>? LogHandler { get; }
}
