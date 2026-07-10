using JorgeCostaMacia.Bus.RabbitMQ.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

/// <summary>In-memory retry scheduler capturing every parked retry, with an optional failure to throw.</summary>
internal sealed class RetrySchedulerFake : IRetryScheduler
{
    public List<(string Exchange, string Queue, byte[] Body, IReadOnlyDictionary<string, object?> Headers, DateTime ScheduledAt)> Scheduled { get; } = [];

    public Exception? Failure { get; set; }

    public Task Schedule(string exchange, string queue, ReadOnlyMemory<byte> body, IReadOnlyDictionary<string, object?> headers, DateTime scheduledAt, CancellationToken cancellationToken)
    {
        if (Failure is not null) throw Failure;

        Scheduled.Add((exchange, queue, body.ToArray(), headers, scheduledAt));

        return Task.CompletedTask;
    }
}
