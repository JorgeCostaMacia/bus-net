using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The bus's logging: the librdkafka callbacks (connection-level and fatal errors, and the client's
/// internal logs mapped to their severity — routed to the logger with the details as scope
/// properties, so the producer and consumer builders map their handlers directly) and the delivery
/// logging scope the consumers open per message.
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
    /// Opens the logging scope carrying the whole delivery — the partition/offset pointer to refetch
    /// it, the raw body and every envelope header decoded to its type — so every log inside it (the
    /// handler's own included, and the failure lanes) is fully traced and a failed message can be
    /// inspected and reprocessed from the log platform.
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

        foreach (IHeader header in result.Message.Headers)
        {
            byte[] value = header.GetValueBytes();

            context[header.Key] = TransportHeaders.GuidHeaders.Contains(header.Key) && value.Length == 16
                ? new Guid(value)
                : TransportHeaders.IntHeaders.Contains(header.Key) && int.TryParse(Encoding.UTF8.GetString(value), out int count)
                    ? count
                    : Encoding.UTF8.GetString(value);
        }

        return logger.BeginScope(context);
    }
}
