using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka client's librdkafka callback logging: connection-level and fatal errors, and the client's
/// internal logs mapped to their severity — routed to the logger with the details as scope
/// properties, so the producer and consumer builders map their handlers directly.
/// </summary>
internal static class ClientLogger
{
    /// <summary>Logs a client error (connection-level or fatal) with the error destructured in the scope.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="error">The Kafka error.</param>
    public static void LogError(ILogger logger, Error error)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["@Error"] = error
        }))
        {
            logger.LogError("Client error.");
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
}
