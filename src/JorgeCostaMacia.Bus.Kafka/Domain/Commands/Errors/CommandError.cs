using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;

/// <summary>
/// The body parked to a command topic's <c>.error</c> when the command's handler fails terminally —
/// MassTransit-style: the original command embedded <b>fully typed</b> (the handler already ran, so
/// it deserialized once — the error handler reuses that instance, never re-deserializes) together
/// with every error detail and the transport details (topic, partition, offset, timestamp,
/// headers), so browsing the error topic shows the whole failure in one place. The header values are
/// a browsable text view; the original envelope (message id, correlation id and the rest of the
/// trace) still travels cloned, byte-exact, in the parked record's headers (with the failure stamped
/// on top), so reinjection tooling keeps working header-side.
/// </summary>
/// <typeparam name="TCommand">The failed command's type.</typeparam>
public sealed record CommandError<TCommand> : IErrorMessage
    where TCommand : Command
{
    /// <summary>The failure that exhausted the delivery, modeled with its whole inner-exception chain.</summary>
    public ErrorInfo Error { get; init; }

    /// <summary>UTC time the failure was parked.</summary>
    public DateTime ErrorOccurredAt { get; init; }

    /// <summary>The delivery's retry count when it exhausted.</summary>
    public int RetryCount { get; init; }

    /// <summary>The consumer group whose handler failed.</summary>
    public string GroupId { get; init; }

    /// <summary>The host (machine) whose consumer failed — identifies the instance/replica for triage.</summary>
    public string MachineName { get; init; }

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

    /// <summary>The original command, fully typed.</summary>
    public TCommand Message { get; init; }

    /// <summary>Creates the parked body over the failure's details and the failed command.</summary>
    /// <param name="error">The failure, modeled with its whole inner-exception chain.</param>
    /// <param name="errorOccurredAt">UTC time the failure was parked.</param>
    /// <param name="retryCount">The delivery's retry count when it exhausted.</param>
    /// <param name="groupId">The consumer group whose handler failed.</param>
    /// <param name="machineName">The host (machine) whose consumer failed.</param>
    /// <param name="topic">The topic the delivery failed on.</param>
    /// <param name="partition">The partition within the topic.</param>
    /// <param name="offset">The offset within the partition.</param>
    /// <param name="timestamp">UTC time Kafka assigned to the message.</param>
    /// <param name="headers">The delivery's headers as browsable text.</param>
    /// <param name="message">The original command, fully typed.</param>
    public CommandError(ErrorInfo error, DateTime errorOccurredAt, int retryCount, string groupId, string machineName, string topic, int partition, long offset, DateTime timestamp, ImmutableList<KeyValuePair<string, string>> headers, TCommand message)
    {
        Error = error;
        ErrorOccurredAt = errorOccurredAt;
        RetryCount = retryCount;
        GroupId = groupId;
        MachineName = machineName;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        Headers = headers;
        Message = message;
    }

    /// <summary>
    /// Builds the parked body from the error context the handler already built — the typed command,
    /// its envelope and its failure, no re-deserialization — stamping the failing group. The mapping
    /// lives here, next to the record it fills; an unreadable envelope throws to the caller, which
    /// hands the delivery to the fault path.
    /// </summary>
    /// <param name="context">The failed delivery's error context.</param>
    /// <param name="groupId">The consumer group whose handler failed.</param>
    /// <returns>The body parked to the error topic.</returns>
    internal static CommandError<TCommand> Create(CommandErrorContext<TCommand> context, string groupId)
        => new CommandError<TCommand>(
            ErrorInfo.Create(context.Error),
            DateTime.UtcNow,
            context.RetryCount,
            groupId,
            Environment.MachineName,
            context.Transport.Topic,
            context.Transport.Partition.Value,
            context.Transport.Offset.Value,
            context.Transport.Timestamp.UtcDateTime,
            context.Transport.DecodeHeaders(),
            context.Message);
}
