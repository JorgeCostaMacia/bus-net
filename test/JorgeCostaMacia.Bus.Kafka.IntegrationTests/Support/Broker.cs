using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;

/// <summary>
/// Raw Confluent.Kafka access for the failure-lane tests: builds bare clients straight against the
/// container from the same <c>Bus:Producer</c> / <c>Bus:Consumer</c> config the fixture builds, and
/// reads back what the bus parked. It deliberately bypasses the bus so the assertions observe the
/// broker's own state — the message truly sitting on <c>{topic}.error</c> / <c>{topic}.fault</c>, the
/// exception the real client raises for a produce it cannot complete — rather than trusting the bus to
/// report on itself.
/// <para>
/// A park lane is a <b>topic</b>, not a queue: a fresh throwaway consumer group at
/// <see cref="AutoOffsetReset.Earliest"/> is subscribed to it and polled, so a reader that joins after
/// the bus parked still reads the message from offset zero — the Kafka analog of RabbitMQ passively
/// re-declaring a lazily-born park queue.
/// </para>
/// </summary>
internal static class Broker
{
    /// <summary>The bare bootstrap the fixture mapped for the container (both bus sections carry the same authority).</summary>
    /// <param name="configuration">The configuration the fixture built.</param>
    /// <returns>The <c>host:port</c> bootstrap list.</returns>
    public static string BootstrapServers(IConfiguration configuration)
        => configuration["Bus:Consumer:BootstrapServers"]!;

    /// <summary>
    /// Waits for a message to land on a park topic born lazily on first park: a bare consumer on a
    /// throwaway group at <see cref="AutoOffsetReset.Earliest"/> subscribes to the topic and polls in
    /// short bounded passes, so a topic that does not exist yet is simply empty until the bus produces
    /// to it — then the reader is assigned its partition and reads the parked message from offset zero.
    /// Returns <see langword="null"/> if nothing is parked within the timeout.
    /// </summary>
    /// <param name="configuration">The bus configuration carrying the mapped bootstrap.</param>
    /// <param name="topic">The park topic to poll (e.g. <c>{topic}.error</c>).</param>
    /// <param name="timeout">How long to wait for the lazily-born topic to hold a message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The parked delivery, or <see langword="null"/> if none arrived in time.</returns>
    public static async Task<ConsumeResult<Ignore, byte[]>?> WaitForParkedAsync(IConfiguration configuration, string topic, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ConsumerConfig config = new ConsumerConfig()
        {
            BootstrapServers = BootstrapServers(configuration),
            SecurityProtocol = SecurityProtocol.Plaintext,
            // A throwaway group per read, so no stored offset ever fences the read — the reader always
            // starts from the beginning of the park topic.
            GroupId = "readback-" + Guid.NewGuid().ToString("N"),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false,
            AllowAutoCreateTopics = true
        };

        using IConsumer<Ignore, byte[]> consumer = new ConsumerBuilder<Ignore, byte[]>(config).Build();

        consumer.Subscribe(topic);

        DateTime deadline = DateTime.UtcNow.Add(timeout);

        try
        {
            while (DateTime.UtcNow < deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    // A short bounded poll: returns null on an empty pass (the topic has no message
                    // yet), a delivery once the parked message is readable.
                    ConsumeResult<Ignore, byte[]>? result = consumer.Consume(TimeSpan.FromMilliseconds(500));

                    if (result?.Message is not null)
                    {
                        return result;
                    }
                }
                catch (ConsumeException exception) when (!exception.Error.IsFatal)
                {
                    // The park topic is not born yet — the broker answers the subscription with
                    // "unknown topic or partition"; the next pass retries once the bus has produced to
                    // it (the Kafka analog of re-declaring a lazily-born RabbitMQ park queue).
                    await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                }
            }

            return null;
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>Builds a bare producer to inject bytes straight onto a topic, bypassing the bus (its envelope, its stamping) — the malformed body of the fault test.</summary>
    /// <param name="configuration">The bus configuration carrying the mapped bootstrap.</param>
    /// <returns>A bare producer the caller disposes.</returns>
    public static IProducer<Null, byte[]> Producer(IConfiguration configuration)
    {
        ProducerConfig config = new ProducerConfig()
        {
            BootstrapServers = BootstrapServers(configuration),
            SecurityProtocol = SecurityProtocol.Plaintext
        };

        return new ProducerBuilder<Null, byte[]>(config).Build();
    }

    /// <summary>
    /// Creates a topic capped to a tiny <c>max.message.bytes</c>, so a later oversized produce is
    /// rejected by the broker — the deterministic trigger for the produce-failure test. Awaits the
    /// admin round-trip, so the topic exists before the caller produces to it.
    /// </summary>
    /// <param name="configuration">The bus configuration carrying the mapped bootstrap.</param>
    /// <param name="topic">The topic to create.</param>
    /// <param name="maxMessageBytes">The per-topic message-size cap that makes the produce oversized.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public static async Task CreateCappedTopicAsync(IConfiguration configuration, string topic, int maxMessageBytes, CancellationToken cancellationToken)
    {
        AdminClientConfig config = new AdminClientConfig()
        {
            BootstrapServers = BootstrapServers(configuration),
            SecurityProtocol = SecurityProtocol.Plaintext
        };

        using IAdminClient admin = new AdminClientBuilder(config).Build();

        // Bound the admin round-trip generously: the default controller-wait is ~60s, which the client
        // can exhaust ("Failed while waiting for controller: Local_TimedOut") when this suite runs
        // alongside the other broker containers and controller discovery is slow under that load. A
        // 90s request/operation window absorbs that contention while staying bounded.
        CreateTopicsOptions options = new CreateTopicsOptions()
        {
            RequestTimeout = TimeSpan.FromSeconds(90),
            OperationTimeout = TimeSpan.FromSeconds(90)
        };

        await admin.CreateTopicsAsync(
            new TopicSpecification[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 1,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string> { ["max.message.bytes"] = maxMessageBytes.ToString(CultureInfo.InvariantCulture) }
                }
            },
            options);

        // CreateTopicsAsync completing does not guarantee cancellation observability, so honor the token here.
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>Reads a header's text from a parked delivery — the bus writes the <c>jcm-error-*</c> headers as UTF-8 bytes, so the last value under the key is decoded as UTF-8.</summary>
    /// <param name="result">The parked delivery.</param>
    /// <param name="key">The header key.</param>
    /// <returns>The header text, or <see langword="null"/> when the header is absent.</returns>
    public static string? Header(ConsumeResult<Ignore, byte[]> result, string key)
    {
        if (!result.Message.Headers.TryGetLastBytes(key, out byte[] bytes))
        {
            return null;
        }

        return Encoding.UTF8.GetString(bytes);
    }
}
