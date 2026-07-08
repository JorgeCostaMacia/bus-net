using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests.Fakes;

/// <summary>In-memory outbound gate capturing every produced message — the seam the retry job re-produces through.</summary>
internal sealed class ProducerFake : IProducer
{
    public List<(string Topic, Message<Null, byte[]> Message)> Produced { get; } = [];

    /// <summary>When set, every produce throws it — the broker-down seam.</summary>
    public Exception? Failure { get; set; }

    public Task Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        if (Failure is not null) throw Failure;

        Produced.Add((topic, message));

        return Task.CompletedTask;
    }

    public Task Produce(IEnumerable<KeyValuePair<string, Message<Null, byte[]>>> messages, CancellationToken cancellationToken = default)
        => Task.WhenAll(messages.Select(message => Produce(message.Key, message.Value, cancellationToken)));
}
