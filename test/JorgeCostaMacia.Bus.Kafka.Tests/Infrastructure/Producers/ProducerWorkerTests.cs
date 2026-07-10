using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ProducerWorkerTests
{
    private readonly KafkaProducerFake _kafka = new();

    private ProducerWorker Sut() => new(_kafka, NullLogger<ProducerWorker>.Instance);

    [Fact]
    public async Task StopAsync_FlushesTheProducer()
    {
        await Sut().StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, _kafka.Flushes);
    }

    [Fact]
    public async Task StopAsync_FlushInterrupted_IsSwallowed()
    {
        _kafka.FlushFailure = new OperationCanceledException();

        await Sut().StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, _kafka.Flushes);
    }

    [Fact]
    public async Task StopAsync_FlushFails_IsSwallowed()
    {
        // a flush failure at shutdown is logged, never thrown — failing the host's stop would not
        // save the queued messages anyway.
        _kafka.FlushFailure = new InvalidOperationException("boom");

        await Sut().StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, _kafka.Flushes);
    }

    [Fact]
    public async Task StartAsync_DoesNothing()
    {
        await Sut().StartAsync(TestContext.Current.CancellationToken);

        Assert.Empty(_kafka.Produced);
        Assert.Equal(0, _kafka.Flushes);
    }
}
