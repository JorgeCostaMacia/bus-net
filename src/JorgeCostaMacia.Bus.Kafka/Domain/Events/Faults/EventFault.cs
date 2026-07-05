using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;

/// <summary>
/// The body parked to an event topic's <c>.fault</c> — the broken delivery's transport details
/// (topic, partition, offset, timestamp, headers) together with every error detail and the raw body
/// as text, so browsing the fault topic shows where it broke, why, and what arrived — without ever
/// deserializing the message (it is the thing that could not be trusted; for the same reason it
/// carries no traced metadata — the envelope may be the very thing that broke). The original envelope
/// still travels cloned, byte-exact, in the parked record's headers (with the failure stamped on top).
/// </summary>
public sealed record EventFault : IErrorMessage
{
    /// <summary>Full type name of the exception that broke the delivery.</summary>
    public string ErrorType { get; init; }

    /// <summary>The exception's message.</summary>
    public string ErrorMessage { get; init; }

    /// <summary>The exception's stack trace, when available.</summary>
    public string? ErrorStackTrace { get; init; }

    /// <summary>UTC time the failure was parked.</summary>
    public DateTime ErrorOccurredAt { get; init; }

    /// <summary>The consumer group whose delivery broke.</summary>
    public string GroupId { get; init; }

    /// <summary>The topic the delivery broke on.</summary>
    public string Topic { get; init; }

    /// <summary>The partition within the topic where the broken message is stored.</summary>
    public int Partition { get; init; }

    /// <summary>The offset of the broken message within the partition — its unique position in the log.</summary>
    public long Offset { get; init; }

    /// <summary>UTC time Kafka assigned to the message (producer time or log-append time).</summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    /// The delivery's headers as best-effort UTF-8 text — every header in order, duplicate keys
    /// preserved (Kafka allows them).
    /// </summary>
    public ImmutableList<KeyValuePair<string, string>> Headers { get; init; }

    /// <summary>The delivered raw body as text — never deserialized.</summary>
    public string Message { get; init; }

    /// <summary>Creates the parked body over the failure's details and the broken delivery.</summary>
    /// <param name="errorType">Full type name of the exception.</param>
    /// <param name="errorMessage">The exception's message.</param>
    /// <param name="errorStackTrace">The exception's stack trace, when available.</param>
    /// <param name="errorOccurredAt">UTC time the failure was parked.</param>
    /// <param name="groupId">The consumer group whose delivery broke.</param>
    /// <param name="topic">The topic the delivery broke on.</param>
    /// <param name="partition">The partition within the topic.</param>
    /// <param name="offset">The offset within the partition.</param>
    /// <param name="timestamp">UTC time Kafka assigned to the message.</param>
    /// <param name="headers">The delivery's headers as best-effort UTF-8 text.</param>
    /// <param name="message">The delivered raw body as text.</param>
    public EventFault(string errorType, string errorMessage, string? errorStackTrace, DateTime errorOccurredAt, string groupId, string topic, int partition, long offset, DateTime timestamp, ImmutableList<KeyValuePair<string, string>> headers, string message)
    {
        ErrorType = errorType;
        ErrorMessage = errorMessage;
        ErrorStackTrace = errorStackTrace;
        ErrorOccurredAt = errorOccurredAt;
        GroupId = groupId;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        Headers = headers;
        Message = message;
    }

    /// <summary>
    /// Builds the parked body from the fault context the consumer already built — the raw body as
    /// text, the transport's consume details mapped over, every header decoded as best-effort UTF-8
    /// text and the failure — stamping the failing group. The mapping lives here, next to the record
    /// it fills.
    /// </summary>
    /// <param name="context">The broken delivery's fault context.</param>
    /// <param name="groupId">The consumer group whose delivery broke.</param>
    /// <returns>The body parked to the fault topic.</returns>
    internal static EventFault Create(EventFaultContext context, string groupId)
    {
        Type type = context.Error.GetType();

        return new(
            type.FullName ?? type.Name,
            context.Error.Message,
            context.Error.StackTrace,
            DateTime.UtcNow,
            groupId,
            context.Transport.Topic,
            context.Transport.Partition.Value,
            context.Transport.Offset.Value,
            context.Transport.Timestamp.UtcDateTime,
            context.Transport.DecodeHeaders(),
            context.Message);
    }
}
