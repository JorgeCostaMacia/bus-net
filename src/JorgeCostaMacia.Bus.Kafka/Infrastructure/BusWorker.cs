using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Hosts the bus's lifecycle in the application: on shutdown it flushes the bus's producer, waiting
/// until every queued message is delivered (librdkafka batches sends internally), so stopping the
/// app never drops messages that were still in the outbound buffer.
/// </summary>
internal sealed class BusWorker : IHostedService
{
    private readonly Bus _bus;
    private readonly ILogger<BusWorker> _logger;

    /// <summary>Creates the worker over the bus whose lifecycle it manages.</summary>
    /// <param name="bus">The bus.</param>
    /// <param name="logger">The logger for an interrupted flush.</param>
    public BusWorker(Bus bus, ILogger<BusWorker> logger)
    {
        _bus = bus;
        _logger = logger;
    }

    /// <summary>Nothing to start — the bus's producer connects lazily on first use.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Flushes the bus on shutdown: waits until the outbound queue is fully delivered. When the
    /// shutdown's grace period runs out first, the interruption is logged instead of failing the
    /// host's stop — queued messages may be lost.
    /// </summary>
    /// <param name="cancellationToken">A token bounding how long the flush may wait.</param>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _bus.Flush(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            using (BusLogger.Action(_logger, BusLogger.Actions.FlushCanceled)) _logger.LogWarning("Flush canceled; queued messages may be lost.");
        }

        return Task.CompletedTask;
    }
}
