using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>In-memory retry scheduler capturing every parked retry, with an optional failure to throw.</summary>
internal sealed class RetrySchedulerFake : IRetryScheduler
{
    public List<(string Topic, byte[] Body, Headers Headers, DateTime ScheduledAt)> Scheduled { get; } = [];

    public Exception? Failure { get; set; }

    public Task Schedule(string topic, byte[] body, Headers headers, DateTime scheduledAt, CancellationToken cancellationToken)
    {
        if (Failure is not null) throw Failure;

        Scheduled.Add((topic, body, headers, scheduledAt));

        return Task.CompletedTask;
    }
}
