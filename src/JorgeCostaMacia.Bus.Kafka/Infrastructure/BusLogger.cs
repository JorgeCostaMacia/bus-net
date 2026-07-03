using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The bus's logging: the librdkafka callbacks (connection-level and fatal errors, and the client's
/// internal logs mapped to their severity — routed to the logger with the details as scope
/// properties, so the producer and consumer builders map their handlers directly), the logging
/// scopes (the worker's identity, the inbound delivery, the outbound delivery — body and envelope
/// decoded, inspectable and reinjectable from the log platform) and the retry warning.
/// </summary>
internal static class BusLogger
{
    /// <summary>Logs a bus client error (connection-level or fatal) with the error destructured in the scope.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="error">The Kafka error.</param>
    public static void LogError(ILogger logger, Error error)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["@Error"] = error
        }))
        {
            logger.LogError("Bus error.");
        }
    }

    /// <summary>Logs an internal (librdkafka) message with its severity and source in the scope.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="log">The Kafka log message.</param>
    public static void Log(ILogger logger, LogMessage log)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["Name"] = log.Name,
            ["Facility"] = log.Facility
        }))
        {
            logger.Log((LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging), "{Message}", log.Message);
        }
    }

    /// <summary>
    /// Opens the logging scope carrying the consumer worker's identity — topic and group id — for the
    /// whole life of its loop.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="topic">The Kafka topic the worker consumes.</param>
    /// <param name="groupId">The worker's consumer group id.</param>
    /// <returns>The scope to dispose when the loop ends.</returns>
    public static IDisposable? WorkerContext(ILogger logger, string topic, string groupId)
        => logger.BeginScope(new Dictionary<string, object?>
        {
            ["Topic"] = topic,
            ["GroupId"] = groupId
        });

    /// <summary>
    /// Opens the logging scope carrying the whole inbound delivery — the partition/offset pointer to
    /// refetch it, the raw body and every envelope header decoded to its type — so every log inside
    /// it (the handler's own included, and the failure lanes) is fully traced and a failed message
    /// can be inspected and reprocessed from the log platform.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="result">The delivered message.</param>
    /// <returns>The scope to dispose when the delivery's iteration ends.</returns>
    public static IDisposable? ConsumerContext(ILogger logger, ConsumeResult<Null, byte[]> result)
    {
        Dictionary<string, object?> context = new()
        {
            ["Partition"] = result.Partition.Value,
            ["Offset"] = result.Offset.Value,
            ["Body"] = result.Message.Value is null ? null : Encoding.UTF8.GetString(result.Message.Value)
        };

        Decode(context, result.Message.Headers);

        return logger.BeginScope(context);
    }

    /// <summary>
    /// Opens the logging scope carrying the whole outbound delivery — topic, raw body and every
    /// envelope header decoded to its type — so a failed send can be inspected and reprocessed from
    /// the log platform.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="topic">The Kafka topic produced to.</param>
    /// <param name="message">The outbound message.</param>
    /// <returns>The scope to dispose after logging the failure.</returns>
    public static IDisposable? ProducerContext(ILogger logger, string topic, Message<Null, byte[]> message)
    {
        Dictionary<string, object?> context = new()
        {
            ["Topic"] = topic,
            ["Body"] = message.Value is null ? null : Encoding.UTF8.GetString(message.Value)
        };

        Decode(context, message.Headers);

        return logger.BeginScope(context);
    }

    /// <summary>Logs a failed handling requeued to retry, with the retry number in the scope.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the requeued delivery.</param>
    public static void LogRetry(ILogger logger, Exception exception, int retry)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["Retry"] = retry
        }))
        {
            logger.LogWarning(exception, "Handling failed; requeued to retry.");
        }
    }

    /// <summary>Decodes every envelope header into the scope — Guids and counters typed, the rest as text.</summary>
    /// <param name="context">The scope dictionary to fill.</param>
    /// <param name="headers">The delivery's headers.</param>
    private static void Decode(Dictionary<string, object?> context, Headers headers)
    {
        foreach (IHeader header in headers)
        {
            byte[] value = header.GetValueBytes();

            context[header.Key] = TransportHeaders.GuidHeaders.Contains(header.Key) && value.Length == 16
                ? new Guid(value)
                : TransportHeaders.IntHeaders.Contains(header.Key) && int.TryParse(Encoding.UTF8.GetString(value), out int count)
                    ? count
                    : Encoding.UTF8.GetString(value);
        }
    }
}
