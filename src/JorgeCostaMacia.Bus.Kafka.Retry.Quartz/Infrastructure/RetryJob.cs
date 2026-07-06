using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
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
/// Quartz's dependency-injection job factory; a produce failure propagates so Quartz applies its
/// misfire / recovery policy.
/// </summary>
internal sealed class RetryJob : IJob
{
    public const string TOPIC_KEY = "topic";
    public const string BODY_KEY = "body";
    public const string HEADERS_KEY = "headers";

    private readonly IProducer _producer;

    /// <summary>Creates the job over the bus's outbound producer gate.</summary>
    /// <param name="producer">The outbound gate the parked retry is re-produced through.</param>
    public RetryJob(IProducer producer)
    {
        _producer = producer;
    }

    /// <summary>Reads the parked retry and re-produces it to its original topic.</summary>
    /// <param name="context">The Quartz execution context carrying the job's data map.</param>
    public async Task Execute(IJobExecutionContext context)
    {
        string topic = Topic(context.MergedJobDataMap);
        byte[] body = Body(context.MergedJobDataMap);
        Headers headers = Headers(context.MergedJobDataMap);

        await _producer.Produce(topic, new Message<Null, byte[]> { Value = body, Headers = headers }, context.CancellationToken);
    }

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
