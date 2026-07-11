using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;

/// <summary>
/// The Kafka client's logging — the librdkafka callbacks (connection-level and fatal errors, the
/// client's internal logs mapped to their severity, and the opt-in statistics), routed to the logger
/// with the details as <see cref="LogContext"/> properties so the producer and consumer builders map
/// their handlers directly. Everything logs under the dedicated <see cref="Category"/>, separate
/// from the bus's own categories, so the client's noise is silenced independently.
/// </summary>
internal static class KafkaLogger
{
    /// <summary>
    /// The logger category every librdkafka callback logs under (e.g. silence it with
    /// <c>Logging:LogLevel:JorgeCostaMacia.Bus.Kafka.Client = Warning</c>).
    /// </summary>
    public const string Category = "JorgeCostaMacia.Bus.Kafka.Client";

    /// <summary>
    /// Logs a Kafka client error with the error destructured in the context — critical when the client
    /// is in an unrecoverable state (<see cref="Error.IsFatal"/>), a warning otherwise (the client
    /// recovers on its own; the docs call these informational).
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="error">The Kafka error.</param>
    public static void LogError(ILogger logger, Error error)
    {
        using (LogContext.Push(new PropertyEnricher("KafkaError", error, destructureObjects: true)))
        {
            logger.Log(error.IsFatal ? LogLevel.Critical : LogLevel.Warning, "Kafka error.");
        }
    }

    /// <summary>
    /// Logs an internal (librdkafka) message with its source and text in the context, mapped to its
    /// severity but capped at <see cref="LogLevel.Error"/> — the client's crit/alert/emerg facilities
    /// are not a fatal host state (the client recovers), so the passthrough never emits
    /// <see cref="LogLevel.Critical"/>. That level is reserved for a dying host, and the separate error
    /// handler (<see cref="LogError"/>) owns the fatal → <c>StopApplication</c> path.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="log">The Kafka log message.</param>
    public static void Log(ILogger logger, LogMessage log)
    {
        using (LogContext.Push(
            new PropertyEnricher("KafkaName", log.Name),
            new PropertyEnricher("KafkaFacility", log.Facility),
            new PropertyEnricher("KafkaMessage", log.Message)))
        {
            LogLevel level = (LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);

            // crit-facility internal logs are informational, not fatal (ordering is
            // Trace < Debug < Information < Warning < Error < Critical): cap them at Error so Critical
            // stays reserved for the host dying, while leaving the None sentinel untouched.
            if (level > LogLevel.Error && level != LogLevel.None) level = LogLevel.Error;

            logger.Log(level, "Kafka log.");
        }
    }

    /// <summary>Logs the client's statistics JSON — emitted only when <c>StatisticsIntervalMs</c> is configured.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="statistics">The librdkafka statistics JSON.</param>
    public static void LogStatistics(ILogger logger, string statistics)
    {
        using (LogContext.PushProperty("KafkaStatistics", statistics))
        {
            logger.LogDebug("Kafka statistics.");
        }
    }
}
