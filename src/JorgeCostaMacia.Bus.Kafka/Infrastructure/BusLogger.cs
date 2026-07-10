using System.Globalization;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core;
using Serilog.Core.Enrichers;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The bus's logging: the contexts (the worker's identity, the inbound delivery, the outbound
/// delivery — body and envelope decoded, inspectable and reinjectable from the log platform), the
/// retry warnings, the commit failures and the partition lifecycle. Every outcome log carries the
/// minimal, groupable template; the variable data travels through Serilog's
/// <see cref="LogContext"/> — the accepted core coupling, the host enriches
/// <c>FromLogContext</c> — with the failures expanded by a <c>BusDescription</c> from
/// <see cref="BusLoggerDescriptions"/>, so they are indexable and manageable from the log platform.
/// The Kafka client's own callbacks log apart, through <see cref="KafkaLogger"/>.
/// </summary>
internal static class BusLogger
{
    /// <summary>
    /// Opens the context stamping the outcome's <c>BusDescription</c> (one of
    /// <see cref="BusLoggerDescriptions"/>) on the log written inside it.
    /// </summary>
    /// <param name="description">The expansion, from <see cref="BusLoggerDescriptions"/>.</param>
    /// <returns>The context to dispose after logging the outcome.</returns>
    public static IDisposable DescriptionContext(string description)
        => LogContext.PushProperty("BusDescription", description);

    /// <summary>
    /// Logs a failed background offset commit — a silent failure otherwise: the stored offsets stay
    /// uncommitted and the crash-redelivery window widens. Successful commits are not logged.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="committed">The commit result reported by the client.</param>
    public static void LogCommit(ILogger logger, CommittedOffsets committed)
    {
        if (!committed.Error.IsError && committed.Offsets.All(offset => !offset.Error.IsError)) return;

        using (LogContext.Push(
            new PropertyEnricher("KafkaError", committed.Error, destructureObjects: true),
            new PropertyEnricher("KafkaOffsets", string.Join(", ", committed.Offsets.Where(offset => offset.Error.IsError))),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.RedeliveryWindowWidened)))
        {
            logger.LogWarning("Commit failed.");
        }
    }

    /// <summary>Logs the partitions incrementally assigned to the worker in a rebalance.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="partitions">The assigned partitions.</param>
    public static void LogPartitionsAssigned(ILogger logger, List<TopicPartition> partitions)
    {
        using (LogContext.PushProperty("KafkaPartitions", string.Join(", ", partitions)))
        {
            logger.LogInformation("Partitions assigned.");
        }
    }

    /// <summary>Logs the partitions incrementally revoked from the worker in a rebalance.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="partitions">The revoked partitions.</param>
    public static void LogPartitionsRevoked(ILogger logger, List<TopicPartitionOffset> partitions)
    {
        using (LogContext.PushProperty("KafkaPartitions", string.Join(", ", partitions)))
        {
            logger.LogInformation("Partitions revoked.");
        }
    }

    /// <summary>Logs the partitions the worker lost by falling out of the group — their new owners redeliver.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="partitions">The lost partitions.</param>
    public static void LogPartitionsLost(ILogger logger, List<TopicPartitionOffset> partitions)
    {
        using (LogContext.Push(
            new PropertyEnricher("KafkaPartitions", string.Join(", ", partitions)),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.RedeliveredToNewOwner)))
        {
            logger.LogWarning("Partitions lost.");
        }
    }

    /// <summary>
    /// Opens the logging context carrying the consumer worker's identity — topic and group id — for
    /// the whole life of its loop.
    /// </summary>
    /// <param name="topic">The Kafka topic the worker consumes.</param>
    /// <param name="groupId">The worker's consumer group id.</param>
    /// <returns>The context to dispose when the loop ends.</returns>
    public static IDisposable WorkerContext(string topic, string groupId)
        => LogContext.Push(
            new PropertyEnricher("Topic", topic),
            new PropertyEnricher("GroupId", groupId));

    /// <summary>
    /// Opens the logging context carrying the whole inbound delivery — the partition/offset pointer to
    /// refetch it, the raw body and every envelope header decoded to its type — so every log inside
    /// it (the handler's own included, and the failure lanes) is fully traced and a failed message
    /// can be inspected and reprocessed from the log platform.
    /// </summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>The context to dispose when the delivery's iteration ends.</returns>
    public static IDisposable ConsumerContext(ConsumeResult<Ignore, byte[]> result)
    {
        List<ILogEventEnricher> context =
        [
            new PropertyEnricher("Partition", result.Partition.Value),
            new PropertyEnricher("Offset", result.Offset.Value),
            new PropertyEnricher("Body", result.Message.Value is null ? null : Encoding.UTF8.GetString(result.Message.Value))
        ];

        Decode(context, result.Message.Headers);

        return LogContext.Push(context.ToArray());
    }

    /// <summary>
    /// Opens the logging context carrying the whole outbound delivery — topic, raw body and every
    /// envelope header decoded to its type — so a failed send can be inspected and reprocessed from
    /// the log platform.
    /// </summary>
    /// <param name="topic">The Kafka topic produced to.</param>
    /// <param name="message">The outbound message.</param>
    /// <returns>The context to dispose after logging the failure.</returns>
    public static IDisposable ProducerContext(string topic, Message<Null, byte[]> message)
    {
        List<ILogEventEnricher> context =
        [
            new PropertyEnricher("Topic", topic),
            new PropertyEnricher("Body", message.Value is null ? null : Encoding.UTF8.GetString(message.Value))
        ];

        Decode(context, message.Headers);

        return LogContext.Push(context.ToArray());
    }

    /// <summary>Logs a failed handling requeued to retry, with the retry number in the context.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the requeued delivery.</param>
    public static void LogRetry(ILogger logger, Exception exception, int retry)
    {
        using (LogContext.Push(
            new PropertyEnricher("Retry", retry),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.RequeuedToRetry)))
        {
            logger.LogWarning(exception, "Handler failed.");
        }
    }

    /// <summary>Logs a failed handling parked for a delayed retry, with the retry number and its time in the context.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the parked delivery.</param>
    /// <param name="scheduledAt">The UTC time the retry is produced back at.</param>
    public static void LogRetry(ILogger logger, Exception exception, int retry, DateTime scheduledAt)
    {
        using (LogContext.Push(
            new PropertyEnricher("Retry", retry),
            new PropertyEnricher("ScheduledAt", scheduledAt),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.ScheduledToRetry)))
        {
            logger.LogWarning(exception, "Handler failed.");
        }
    }

    /// <summary>Decodes every envelope header into the context — Guids and counters typed, the rest as text, a duplicated key keeping its last value. A message with no headers (never set) contributes nothing rather than throwing — logging must never fault, least of all in a catch about to rethrow.</summary>
    /// <param name="context">The enricher list to fill.</param>
    /// <param name="headers">The delivery's headers, or <see langword="null"/> when the message carries none.</param>
    private static void Decode(List<ILogEventEnricher> context, Headers? headers)
    {
        if (headers is null) return;

        Dictionary<string, object?> decoded = new Dictionary<string, object?>();

        foreach (IHeader header in headers)
        {
            byte[] value = header.GetValueBytes();

            decoded[header.Key] = TransportHeaders.GuidHeaders.Contains(header.Key) && value.Length == 16
                ? new Guid(value)
                : TransportHeaders.IntHeaders.Contains(header.Key) && int.TryParse(Encoding.UTF8.GetString(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out int count)
                    ? count
                    : Encoding.UTF8.GetString(value);
        }

        context.AddRange(decoded.Select(entry => new PropertyEnricher(entry.Key, entry.Value)));
    }
}
