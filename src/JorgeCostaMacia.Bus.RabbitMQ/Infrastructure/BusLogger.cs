using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The bus's logging: the scopes (the worker's identity, and the inbound delivery — body and envelope
/// decoded, inspectable and reinjectable from the log platform) and the retry warnings. Every outcome
/// log carries the minimal, groupable template and a <c>BusDescription</c> expansion from
/// <see cref="BusLoggerDescriptions"/>, so the failures are indexable and manageable from the log
/// platform.
/// </summary>
internal static class BusLogger
{
    /// <summary>
    /// Opens a scope stamping the outcome's <c>BusDescription</c> (one of
    /// <see cref="BusLoggerDescriptions"/>) on the log written inside it.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="description">The expansion, from <see cref="BusLoggerDescriptions"/>.</param>
    /// <returns>The scope to dispose after logging the outcome.</returns>
    public static IDisposable? DescriptionContext(ILogger logger, string description)
        => logger.BeginScope(new Dictionary<string, object?>
        {
            ["BusDescription"] = description
        });

    /// <summary>
    /// Opens the logging scope carrying the consumer worker's identity — the exchange it binds to and
    /// the queue it consumes — for the whole delivery.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exchange">The exchange the worker's queue binds to.</param>
    /// <param name="queue">The queue the worker consumes.</param>
    /// <returns>The scope to dispose when the delivery ends.</returns>
    public static IDisposable? WorkerContext(ILogger logger, string exchange, string queue)
        => logger.BeginScope(new Dictionary<string, object?>
        {
            ["Exchange"] = exchange,
            ["Queue"] = queue
        });

    /// <summary>
    /// Opens the logging scope carrying the whole inbound delivery — the routing key, delivery tag and
    /// redelivered flag, the raw body and every envelope header decoded to its type — so every log
    /// inside it (the handler's own included, and the failure lanes) is fully traced and a failed
    /// message can be inspected from the log platform.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="args">The delivered message.</param>
    /// <returns>The scope to dispose when the delivery's iteration ends.</returns>
    public static IDisposable? ConsumerContext(ILogger logger, BasicDeliverEventArgs args)
    {
        Dictionary<string, object?> context = new()
        {
            ["RoutingKey"] = args.RoutingKey,
            ["DeliveryTag"] = args.DeliveryTag,
            ["Redelivered"] = args.Redelivered,
            ["Body"] = args.Body.Length == 0 ? null : Encoding.UTF8.GetString(args.Body.Span)
        };

        Decode(context, args.BasicProperties.Headers);

        return logger.BeginScope(context);
    }

    /// <summary>Logs a failed handling republished to retry, with the retry number in the scope.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the republished delivery.</param>
    public static void LogRetry(ILogger logger, Exception exception, int retry)
    {
        using (logger.BeginScope(new Dictionary<string, object?>
        {
            ["Retry"] = retry,
            ["BusDescription"] = BusLoggerDescriptions.RepublishedToRetry
        }))
        {
            logger.LogWarning(exception, "Handler failed.");
        }
    }

    /// <summary>Decodes every envelope header into the scope — Guids and counters typed, the rest as text. A delivery with no headers contributes nothing rather than throwing — logging must never fault, least of all in a catch about to rethrow.</summary>
    /// <param name="context">The scope dictionary to fill.</param>
    /// <param name="headers">The delivery's headers, or <see langword="null"/> when the message carries none.</param>
    private static void Decode(Dictionary<string, object?> context, IDictionary<string, object?>? headers)
    {
        if (headers is null) return;

        foreach ((string key, object? raw) in headers)
        {
            if (raw is not byte[] value)
            {
                context[key] = raw;

                continue;
            }

            context[key] = TransportHeaders.GuidHeaders.Contains(key) && value.Length == 16
                ? new Guid(value)
                : TransportHeaders.IntHeaders.Contains(key) && int.TryParse(Encoding.UTF8.GetString(value), out int count)
                    ? count
                    : Encoding.UTF8.GetString(value);
        }
    }
}
