using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Quartz;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;

/// <summary>
/// The Quartz-backed <see cref="IRetryScheduler"/>: it does not produce the delayed retry itself — it
/// writes a one-shot <see cref="RetryJob"/> to the scheduler's store to be fired at its time by any
/// node in the cluster, which then re-produces the message. It uses whichever
/// <see cref="ISchedulerFactory"/> the application registered; the store, clustering and serialization
/// are the application's Quartz configuration, not this package's — but they must be persistent and
/// shared with the executing fleet, because scheduling commits to the store and the delivery is then
/// acked, so an in-memory store would drop the retry on a restart of the scheduling process. Each
/// parked retry is a one-shot job grouped under its topic and named <c>messageId:retryCount</c> — so a
/// job maps back to the message being retried and the same delivery scheduled twice deduplicates;
/// recovery is requested so a node that dies mid-fire re-runs it (at-least-once, a rare duplicate is
/// preferred over a loss).
/// </summary>
internal sealed class RetryScheduler : IRetryScheduler
{
    private readonly ISchedulerFactory _schedulerFactory;

    /// <summary>Creates the scheduler over the application's Quartz scheduler factory.</summary>
    /// <param name="schedulerFactory">The Quartz scheduler factory the application registered — its store is where parked retries are written.</param>
    public RetryScheduler(ISchedulerFactory schedulerFactory) => _schedulerFactory = schedulerFactory;

    /// <inheritdoc />
    public async Task Schedule(string topic, string groupId, byte[] body, Headers headers, DateTime scheduledAt, CancellationToken cancellationToken)
    {
        IScheduler scheduler = await _schedulerFactory.GetScheduler(cancellationToken);

        string identity = $"{MessageId(headers):N}:{RetryCount(headers)}";

        IJobDetail job = JobBuilder.Create<RetryJob>()
            .WithIdentity(identity, topic)
            .WithDescription(groupId)
            .UsingJobData(RetryJob.TOPIC_KEY, topic)
            .UsingJobData(RetryJob.BODY_KEY, Convert.ToBase64String(body))
            .UsingJobData(RetryJob.HEADERS_KEY, JsonSerializer.Serialize(headers.Select(header => new KeyValuePair<string, byte[]?>(header.Key, header.GetValueBytes()))))
            .RequestRecovery()
            .Build();

        ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity(identity, topic)
            .WithDescription(groupId)
            .StartAt(new DateTimeOffset(DateTime.SpecifyKind(scheduledAt, DateTimeKind.Utc)))
            .Build();

        await scheduler.ScheduleJob(job, trigger, cancellationToken);
    }

    /// <summary>The retried message's id — the store job maps back to the very message (traceable), and the same delivery scheduled twice collides instead of parking two jobs (deduplicated). Falls back to a fresh id if the envelope carries none.</summary>
    private static Guid MessageId(Headers headers)
        => headers.TryGetLastBytes(TransportHeaders.MessageId, out byte[] id)
            ? new Guid(id)
            : GuidFactory.Domain.GuidFactory.Create();

    /// <summary>The retry number this delivery represents — the envelope's already-incremented retry count (0 when absent).</summary>
    private static int RetryCount(Headers headers)
        => headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] retry)
            ? int.Parse(Encoding.UTF8.GetString(retry), CultureInfo.InvariantCulture)
            : 0;
}
