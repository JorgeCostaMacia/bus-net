using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Hosts the shared producer's lifecycle in the application: on shutdown it flushes the producer,
/// waiting until every queued message is delivered (librdkafka batches sends internally), so
/// stopping the app never drops messages that were still in the outbound buffer.
/// </summary>
internal sealed class BusProducer : IHostedService
{
    private readonly IProducer<Null, byte[]> _producer;

    /// <summary>Creates the worker over the shared producer whose lifecycle it manages.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    public BusProducer(IProducer<Null, byte[]> producer)
    {
        _producer = producer;
    }

    /// <summary>Nothing to start — the producer connects lazily on first use.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Flushes the producer on shutdown: waits until the outbound queue is fully delivered.</summary>
    /// <param name="cancellationToken">A token bounding how long the flush may wait.</param>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _producer.Flush(cancellationToken);

        return Task.CompletedTask;
    }
}
