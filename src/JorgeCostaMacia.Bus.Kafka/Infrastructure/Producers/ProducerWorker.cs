using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// Owns the shared producer's lifecycle in the application: on shutdown it flushes it, waiting until
/// every queued message is delivered (librdkafka batches sends internally), so stopping the app never
/// drops messages that were still in the outbound buffer. Registered before the consumers so it stops
/// last — they stop first, then this final flush runs over a producer nothing writes to anymore.
/// </summary>
internal sealed class ProducerWorker : IHostedService
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly ILogger<ProducerWorker> _logger;

    /// <summary>Creates the worker over the shared producer whose lifecycle it manages.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="logger">The logger for an interrupted flush.</param>
    public ProducerWorker(IProducer<Null, byte[]> producer, ILogger<ProducerWorker> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <summary>Nothing to start — the producer connects lazily on first use.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Flushes the producer on shutdown: waits until the outbound queue is fully delivered. When the
    /// shutdown's grace period runs out first, the interruption is logged instead of failing the
    /// host's stop — queued messages may be lost.
    /// </summary>
    /// <param name="cancellationToken">A token bounding how long the flush may wait.</param>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _producer.Flush(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.QueuedMessagesMayBeLost)) _logger.LogWarning("Flush canceled.");
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.QueuedMessagesMayBeLost)) _logger.LogWarning(exception, "Flush failed.");
        }

        return Task.CompletedTask;
    }
}
