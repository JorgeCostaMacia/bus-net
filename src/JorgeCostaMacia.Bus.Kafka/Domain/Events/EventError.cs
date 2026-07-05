using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Events;

/// <summary>
/// The body parked to an event topic's <c>.error</c> when the event's subscriber fails terminally —
/// MassTransit-style: the original event embedded <b>fully typed</b> (the subscriber already ran, so
/// it deserialized once — the error handler reuses that instance, never re-deserializes) together
/// with every error detail and the transport details (topic, partition, offset, timestamp,
/// headers), so browsing the error topic shows the whole failure in one place. A first-class traced
/// message: its id and correlation id are the failed message's, so the failure stays tied to its
/// workflow. The header values are a browsable text view; the original envelope still travels
/// cloned, byte-exact, in the parked record's headers (with the failure stamped on top), so
/// reinjection tooling keeps working header-side.
/// </summary>
/// <typeparam name="TEvent">The failed event's type.</typeparam>
public sealed record EventError<TEvent> : ITracedMessage, IErrorMessage
    where TEvent : Event
{
    /// <summary>The failed message's id — shared, so a failure is found by its message's id.</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Correlation identifier of the failed message's workflow.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC time the failure was parked.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>Full type name of the exception that exhausted the delivery.</summary>
    public string ErrorType { get; init; }

    /// <summary>The exception's message.</summary>
    public string ErrorMessage { get; init; }

    /// <summary>The exception's stack trace, when available.</summary>
    public string? ErrorStackTrace { get; init; }

    /// <summary>The delivery's retry count when it exhausted.</summary>
    public int RetryCount { get; init; }

    /// <summary>The consumer group whose subscriber failed.</summary>
    public string GroupId { get; init; }

    /// <summary>The topic the delivery failed on.</summary>
    public string Topic { get; init; }

    /// <summary>The partition within the topic where the failed message is stored.</summary>
    public int Partition { get; init; }

    /// <summary>The offset of the failed message within the partition — its unique position in the log.</summary>
    public long Offset { get; init; }

    /// <summary>UTC time Kafka assigned to the message (producer time or log-append time).</summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    /// The delivery's headers as browsable text — every header in order, duplicate keys preserved
    /// (Kafka allows them); the envelope's binary GUID headers render as GUIDs.
    /// </summary>
    public ImmutableList<KeyValuePair<string, string>> Headers { get; init; }

    /// <summary>The original event, fully typed.</summary>
    public TEvent Message { get; init; }

    /// <summary>Creates the parked body over the failure's details and the failed event.</summary>
    /// <param name="aggregateId">The failed message's id.</param>
    /// <param name="aggregateCorrelationId">Correlation identifier of the failed message's workflow.</param>
    /// <param name="aggregateOccurredAt">UTC time the failure was parked.</param>
    /// <param name="errorType">Full type name of the exception.</param>
    /// <param name="errorMessage">The exception's message.</param>
    /// <param name="errorStackTrace">The exception's stack trace, when available.</param>
    /// <param name="retryCount">The delivery's retry count when it exhausted.</param>
    /// <param name="groupId">The consumer group whose subscriber failed.</param>
    /// <param name="topic">The topic the delivery failed on.</param>
    /// <param name="partition">The partition within the topic.</param>
    /// <param name="offset">The offset within the partition.</param>
    /// <param name="timestamp">UTC time Kafka assigned to the message.</param>
    /// <param name="headers">The delivery's headers as browsable text.</param>
    /// <param name="message">The original event, fully typed.</param>
    public EventError(Guid aggregateId, Guid aggregateCorrelationId, DateTime aggregateOccurredAt, string errorType, string errorMessage, string? errorStackTrace, int retryCount, string groupId, string topic, int partition, long offset, DateTime timestamp, ImmutableList<KeyValuePair<string, string>> headers, TEvent message)
    {
        AggregateId = aggregateId;
        AggregateCorrelationId = aggregateCorrelationId;
        AggregateOccurredAt = aggregateOccurredAt;
        ErrorType = errorType;
        ErrorMessage = errorMessage;
        ErrorStackTrace = errorStackTrace;
        RetryCount = retryCount;
        GroupId = groupId;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        Headers = headers;
        Message = message;
    }

    /// <summary>
    /// Builds the parked body from the error context the subscriber already built — the typed event,
    /// its envelope and its failure, no re-deserialization — stamping the failing group. The mapping
    /// lives here, next to the record it fills; an unreadable envelope throws to the caller, which
    /// hands the delivery to the fault path.
    /// </summary>
    /// <param name="context">The failed delivery's error context.</param>
    /// <param name="groupId">The consumer group whose subscriber failed.</param>
    /// <returns>The body parked to the error topic.</returns>
    internal static EventError<TEvent> Create(EventErrorContext<TEvent> context, string groupId)
    {
        Type type = context.Error.GetType();

        return new(
            context.AggregateId,
            context.AggregateCorrelationId,
            DateTime.UtcNow,
            type.FullName ?? type.Name,
            context.Error.Message,
            context.Error.StackTrace,
            context.RetryCount,
            groupId,
            context.Transport.Topic,
            context.Transport.Partition.Value,
            context.Transport.Offset.Value,
            context.Transport.Timestamp.UtcDateTime,
            context.Transport.DecodeHeaders(),
            context.Message);
    }
}
