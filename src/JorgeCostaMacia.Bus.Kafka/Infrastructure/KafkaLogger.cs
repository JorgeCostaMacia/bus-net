using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka client's logging — the librdkafka callbacks (connection-level and fatal errors, the
/// client's internal logs mapped to their severity, and the opt-in statistics), routed to the logger
/// with the details as scope properties so the producer and consumer builders map their handlers
/// directly. Everything logs under the dedicated <see cref="Category"/>, separate from the bus's own
/// categories, so the client's noise is silenced independently.
/// </summary>
internal static class KafkaLogger
{
    /// <summary>
    /// The logger category every librdkafka callback logs under (e.g. silence it with
    /// <c>Logging:LogLevel:JorgeCostaMacia.Bus.Kafka.Client = Warning</c>).
    /// </summary>
    public const string Category = "JorgeCostaMacia.Bus.Kafka.Client";

    /// <summary>
    /// Logs a Kafka client error with the error destructured in the scope — critical when the client
    /// is in an unrecoverable state (<see cref="Error.IsFatal"/>), a warning otherwise (the client
    /// recovers on its own; the docs call these informational).
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="error">The Kafka error.</param>
    public static void LogError(ILogger logger, Error error)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["@KafkaError"] = error
        }))
        {
            logger.Log(error.IsFatal ? LogLevel.Critical : LogLevel.Warning, "Kafka error.");
        }
    }

    /// <summary>Logs an internal (librdkafka) message with its source and text in the scope, mapped to its severity.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="log">The Kafka log message.</param>
    public static void Log(ILogger logger, LogMessage log)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["KafkaName"] = log.Name,
            ["KafkaFacility"] = log.Facility,
            ["KafkaMessage"] = log.Message
        }))
        {
            logger.Log((LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging), "Kafka log.");
        }
    }

    /// <summary>Logs the client's statistics JSON — emitted only when <c>StatisticsIntervalMs</c> is configured.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="statistics">The librdkafka statistics JSON.</param>
    public static void LogStatistics(ILogger logger, string statistics)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["KafkaStatistics"] = statistics
        }))
        {
            logger.LogDebug("Kafka statistics.");
        }
    }
}
