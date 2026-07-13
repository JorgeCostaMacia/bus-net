using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Quartz;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;

/// <summary>
/// The parked retry's job: produces the delivery back to its topic each time its repeating trigger
/// fires (see <see cref="RetryScheduler"/>).
/// </summary>
/// <remarks>
/// <para>
/// A successful produce deletes the job — pending repetitions included. A failed produce writes and
/// handles nothing: the exception just bubbles — Quartz wraps it and hands it to any job listeners —
/// and the trigger repeats the produce on its own. The dead-letter needs no code at all: with the
/// attempts exhausted Quartz completes the trigger, and the job — durable — stays parked in the
/// store, trigger-less and visible, re-firable with <c>IScheduler.TriggerJob</c> (a re-fire that
/// fails again just stays parked; one that finally succeeds deletes the job).
/// </para>
/// </remarks>
internal sealed class RetryJob : IJob
{
    public const string TOPIC_KEY = "topic";
    public const string BODY_KEY = "body";
    public const string HEADERS_KEY = "headers";

    private readonly IProducer _producer;

    /// <summary>Creates the job over the outbound producer.</summary>
    /// <param name="producer">The outbound gate — the retry is produced through it.</param>
    public RetryJob(IProducer producer)
    {
        _producer = producer;
    }

    /// <inheritdoc />
    public async Task Execute(IJobExecutionContext context)
    {
        string topic = Topic(context.MergedJobDataMap);
        byte[] body = Body(context.MergedJobDataMap);
        Headers headers = Headers(context.MergedJobDataMap);

        // a failed produce just throws: Quartz wraps it and hands it to the job listeners, and the
        // trigger repeats the produce on its own until the attempts run out — nothing to do here.
        await _producer.Produce(topic, new Message<Null, byte[]> { Value = body, Headers = headers }, context.CancellationToken);

        // produced: the job is done for good — deleting the durable job takes any pending
        // repetition with it (and cleans up a re-fired dead-letter).
        await context.Scheduler.DeleteJob(context.JobDetail.Key, context.CancellationToken);
    }

    private static string Topic(JobDataMap data)
    {
        string? value = data.GetString(TOPIC_KEY);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{TOPIC_KEY}'.");
        }

        return value;
    }

    private static byte[] Body(JobDataMap data)
    {
        string? value = data.GetString(BODY_KEY);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{BODY_KEY}'.");
        }

        return Convert.FromBase64String(value);
    }

    private static Headers Headers(JobDataMap data)
    {
        string? value = data.GetString(HEADERS_KEY);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{HEADERS_KEY}'.");
        }

        Headers headers = new Headers();

        foreach (KeyValuePair<string, byte[]?> header in JsonSerializer.Deserialize<List<KeyValuePair<string, byte[]?>>>(value) ?? [])
        {
            headers.Add(header.Key, header.Value);
        }

        return headers;
    }
}
