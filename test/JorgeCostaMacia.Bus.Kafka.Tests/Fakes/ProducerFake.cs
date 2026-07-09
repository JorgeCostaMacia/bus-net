using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// In-memory outbound gate capturing every produced message, with an optional failure to throw —
/// enough surface for the bus and the consumer failure policy.
/// </summary>
internal sealed class ProducerFake : IProducer
{
    public List<(string Topic, Message<Null, byte[]> Message)> Produced { get; } = [];

    public Exception? Failure { get; set; }

    /// <summary>When non-empty, <see cref="Failure"/> only throws for these topics — a partial outage (e.g. the retry lane down, the fault lane healthy).</summary>
    public HashSet<string> FailingTopics { get; } = [];

    public Task Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        if (Failure is not null && (FailingTopics.Count == 0 || FailingTopics.Contains(topic))) throw Failure;

        Produced.Add((topic, message));

        return Task.CompletedTask;
    }

    public Task Produce(IEnumerable<KeyValuePair<string, Message<Null, byte[]>>> messages, CancellationToken cancellationToken = default)
        => Task.WhenAll(messages.Select(message => Produce(message.Key, message.Value, cancellationToken)));
}
