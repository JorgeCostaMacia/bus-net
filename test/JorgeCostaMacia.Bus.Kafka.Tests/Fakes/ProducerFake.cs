using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// In-memory producer capturing every produced message, with an optional failure to throw — enough
/// surface for the bus and the consumer failure policy; everything else is unreachable from the code
/// under test.
/// </summary>
internal sealed class ProducerFake : IProducer<Null, byte[]>
{
    public List<(string Topic, Message<Null, byte[]> Message)> Produced { get; } = [];

    public Exception? Failure { get; set; }

    public Task<DeliveryResult<Null, byte[]>> ProduceAsync(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        if (Failure is not null) throw Failure;

        Produced.Add((topic, message));

        return Task.FromResult(new DeliveryResult<Null, byte[]> { Topic = topic, Message = message });
    }

    public Task<DeliveryResult<Null, byte[]>> ProduceAsync(TopicPartition topicPartition, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
        => ProduceAsync(topicPartition.Topic, message, cancellationToken);

    public void Produce(string topic, Message<Null, byte[]> message, Action<DeliveryReport<Null, byte[]>>? deliveryHandler = null)
        => throw new NotSupportedException();

    public void Produce(TopicPartition topicPartition, Message<Null, byte[]> message, Action<DeliveryReport<Null, byte[]>>? deliveryHandler = null)
        => throw new NotSupportedException();

    public int Poll(TimeSpan timeout) => 0;

    public int Flush(TimeSpan timeout) => 0;

    public void Flush(CancellationToken cancellationToken = default) { }

    public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

    public void BeginTransaction() => throw new NotSupportedException();

    public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void CommitTransaction() => throw new NotSupportedException();

    public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void AbortTransaction() => throw new NotSupportedException();

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        => throw new NotSupportedException();

    public Handle Handle => throw new NotSupportedException();

    public string Name => nameof(ProducerFake);

    public int AddBrokers(string brokers) => 0;

    public void SetSaslCredentials(string username, string password) { }

    public void Dispose() { }
}
