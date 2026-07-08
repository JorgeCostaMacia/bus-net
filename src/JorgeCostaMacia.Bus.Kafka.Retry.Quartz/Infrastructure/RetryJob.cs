using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;
using Quartz;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;

/// <summary>
/// The one-shot Quartz job a clustered node fires when a parked retry's delay elapses: it reads the
/// topic, body and envelope from its own job data and re-produces the message to its topic through the
/// bus's outbound gate — the envelope already carries the incremented retry count and the consumer
/// targeting, so the normal consumers receive and route it with no extra machinery. It owns the job's
/// data keys — the strings it travels under in a <c>UseProperties = true</c> store; the headers travel
/// as a JSON list of key/value pairs (Confluent's headers cannot be serialized directly — their values
/// live behind a method, not a property), each value base64, a null value kept as null. Instantiated by
/// Quartz's dependency-injection job factory.
/// </summary>
/// <remarks>
/// A produce failure never loses the parked message: the job re-schedules ITSELF (same job, a new
/// one-shot trigger per attempt — its group gains an <c>.error:N</c> suffix and the attempt counter
/// travels in the trigger's data, so the Kafka envelope headers are never touched) at growing delays
/// (<see cref="REEXECUTION_DELAYS"/>). When the ladder is exhausted, the job is made durable and its
/// description gains a <c>.fault:</c> suffix — a dead-letter row in the job store, query-able and
/// re-firable (<c>TriggerJob</c>); on a later successful re-fire the job deletes itself. Nothing is
/// rethrown: outcomes are logged (error per attempt, critical on park).
/// </remarks>
internal sealed class RetryJob : IJob
{
    public const string TOPIC_KEY = "topic";
    public const string BODY_KEY = "body";
    public const string HEADERS_KEY = "headers";
    public const string REEXECUTION_KEY = "reexecution";

    /// <summary>The re-execution delays applied when the produce itself fails, attempt by attempt.</summary>
    public static readonly TimeSpan[] REEXECUTION_DELAYS = [TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(30)];

    private readonly IProducer _producer;
    private readonly ILogger<RetryJob> _logger;

    /// <summary>Creates the job over the bus's outbound producer gate.</summary>
    /// <param name="producer">The outbound gate the parked retry is re-produced through.</param>
    /// <param name="logger">The logger the re-execution and dead-letter outcomes are reported through.</param>
    public RetryJob(IProducer producer, ILogger<RetryJob> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <summary>Reads the parked retry and re-produces it to its original topic; on failure, re-schedules itself or dead-letters the job (see the class remarks).</summary>
    /// <param name="context">The Quartz execution context carrying the job's data map.</param>
    public async Task Execute(IJobExecutionContext context)
    {
        string topic = Topic(context.MergedJobDataMap);
        byte[] body = Body(context.MergedJobDataMap);
        Headers headers = Headers(context.MergedJobDataMap);

        try
        {
            await _producer.Produce(topic, new Message<Null, byte[]> { Value = body, Headers = headers }, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (System.Exception exception)
        {
            await Reexecute(context, topic, exception);

            return;
        }

        // a re-fired dead-letter that finally succeeded: durable jobs are not cleaned up by Quartz.
        if (context.JobDetail.Durable)
        {
            await context.Scheduler.DeleteJob(context.JobDetail.Key, context.CancellationToken);
        }
    }

    /// <summary>Re-schedules this job at the next delay of the ladder, or dead-letters it (durable + <c>.fault:</c> description) when the ladder is exhausted.</summary>
    private async Task Reexecute(IJobExecutionContext context, string topic, System.Exception exception)
    {
        int attempt = Attempt(context.MergedJobDataMap);

        if (attempt >= REEXECUTION_DELAYS.Length)
        {
            IJobDetail parked = context.JobDetail.GetJobBuilder()
                .StoreDurably()
                .WithDescription($"{context.JobDetail.Description}.fault:{exception.Message}")
                .Build();

            await context.Scheduler.AddJob(parked, replace: true, cancellationToken: context.CancellationToken);

            _logger.LogCritical(exception, "Retry job '{Job}' for topic '{Topic}' exhausted its re-execution ladder and was dead-lettered (durable, no triggers).", context.JobDetail.Key, topic);

            return;
        }

        ITrigger trigger = TriggerBuilder.Create()
            .ForJob(context.JobDetail)
            .WithIdentity(context.Trigger.Key.Name, $"{context.JobDetail.Key.Group}.error:{attempt + 1}")
            .UsingJobData(REEXECUTION_KEY, (attempt + 1).ToString(CultureInfo.InvariantCulture))
            .StartAt(DateTimeOffset.UtcNow.Add(REEXECUTION_DELAYS[attempt]))
            .Build();

        await context.Scheduler.ScheduleJob(trigger, context.CancellationToken);

        _logger.LogError(exception, "Retry job '{Job}' for topic '{Topic}' failed to produce; re-execution {Attempt}/{Attempts} scheduled at {StartAt}.", context.JobDetail.Key, topic, attempt + 1, REEXECUTION_DELAYS.Length, trigger.StartTimeUtc);
    }

    /// <summary>The re-execution attempt this firing belongs to — <c>0</c> for the original park, carried in the trigger's data afterwards.</summary>
    private static int Attempt(JobDataMap data)
        => data.TryGetValue(REEXECUTION_KEY, out object? value)
            && int.TryParse(value as string, NumberStyles.Integer, CultureInfo.InvariantCulture, out int attempt)
            ? attempt
            : 0;

    /// <summary>The destination topic — the route the parked retry is re-produced to.</summary>
    private static string Topic(JobDataMap data)
    {
        string? value = data.GetString(TOPIC_KEY);

        if (string.IsNullOrEmpty(value)) throw new InvalidOperationException($"Retry job data is empty '{TOPIC_KEY}'.");

        return value;
    }

    /// <summary>The raw message body, decoded from base64.</summary>
    private static byte[] Body(JobDataMap data)
    {
        string? value = data.GetString(BODY_KEY);

        if (string.IsNullOrEmpty(value)) throw new InvalidOperationException($"Retry job data is empty '{BODY_KEY}'.");

        return Convert.FromBase64String(value);
    }

    /// <summary>The envelope headers, decoded from their JSON list — the scheduler always writes it (an empty list when there are none).</summary>
    private static Headers Headers(JobDataMap data)
    {
        string? value = data.GetString(HEADERS_KEY);

        if (string.IsNullOrEmpty(value)) throw new InvalidOperationException($"Retry job data is empty '{HEADERS_KEY}'.");

        Headers headers = new Headers();

        foreach (KeyValuePair<string, byte[]?> header in JsonSerializer.Deserialize<List<KeyValuePair<string, byte[]?>>>(value) ?? [])
        {
            headers.Add(header.Key, header.Value);
        }

        return headers;
    }
}
