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

    public Task<DeliveryResult<Null, byte[]>> Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        if (Failure is not null) throw Failure;

        Produced.Add((topic, message));

        return Task.FromResult(new DeliveryResult<Null, byte[]> { Topic = topic, Message = message });
    }
}
