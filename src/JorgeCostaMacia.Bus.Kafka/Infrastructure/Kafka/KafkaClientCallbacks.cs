using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;

/// <summary>
/// Wires the librdkafka client-level callbacks — error, log and statistics — identically onto the
/// producer and consumer builders, so the reaction lives in one place: the error is logged and, on an
/// <c>AllBrokersDown</c>, flips the reachability tracker down; a fatal one stops the application; the
/// internal logs and the opt-in statistics pass straight to the Kafka category. Two typed overloads
/// (the client builders share no common base exposing the fluent setters) delegate to one shared
/// error reaction, and the log/statistics handlers pass through to <see cref="KafkaLogger"/>.
/// </summary>
internal static class KafkaClientCallbacks
{
    /// <summary>Wires the client callbacks onto a producer builder and returns it, to allow method chaining.</summary>
    /// <param name="builder">The producer builder.</param>
    /// <param name="logger">The logger the client callbacks log under (the Kafka category).</param>
    /// <param name="health">The broker-reachability tracker, flipped down on an all-brokers-down error.</param>
    /// <param name="lifetime">The application lifetime, stopped on a fatal error.</param>
    /// <returns>The same builder, to allow method chaining.</returns>
    public static ProducerBuilder<Null, byte[]> WithClientCallbacks(this ProducerBuilder<Null, byte[]> builder, ILogger logger, BusHealth health, IHostApplicationLifetime lifetime)
        => builder
            .SetErrorHandler((_, error) => OnError(logger, error, health, lifetime))
            .SetLogHandler((_, log) => KafkaLogger.Log(logger, log))
            .SetStatisticsHandler((_, statistics) => KafkaLogger.LogStatistics(logger, statistics));

    /// <summary>Wires the client callbacks onto a consumer builder and returns it, to allow method chaining.</summary>
    /// <param name="builder">The consumer builder.</param>
    /// <param name="logger">The logger the client callbacks log under (the Kafka category).</param>
    /// <param name="health">The broker-reachability tracker, flipped down on an all-brokers-down error.</param>
    /// <param name="lifetime">The application lifetime, stopped on a fatal error.</param>
    /// <returns>The same builder, to allow method chaining.</returns>
    public static ConsumerBuilder<Ignore, byte[]> WithClientCallbacks(this ConsumerBuilder<Ignore, byte[]> builder, ILogger logger, BusHealth health, IHostApplicationLifetime lifetime)
        => builder
            .SetErrorHandler((_, error) => OnError(logger, error, health, lifetime))
            .SetLogHandler((_, log) => KafkaLogger.Log(logger, log))
            .SetStatisticsHandler((_, statistics) => KafkaLogger.LogStatistics(logger, statistics));

    /// <summary>The shared error reaction: logs the error, flips the reachability tracker down on an <c>AllBrokersDown</c>, and stops the application on a fatal one (every other error the client recovers from on its own).</summary>
    private static void OnError(ILogger logger, Error error, BusHealth health, IHostApplicationLifetime lifetime)
    {
        KafkaLogger.LogError(logger, error);

        if (error.Code == ErrorCode.Local_AllBrokersDown) health.Down();
        if (error.IsFatal) lifetime.StopApplication();
    }
}
