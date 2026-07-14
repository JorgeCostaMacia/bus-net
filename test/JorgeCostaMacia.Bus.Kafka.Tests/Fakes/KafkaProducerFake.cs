using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// In-memory double of the low-level <see cref="IProducer{TKey, TValue}"/> the <c>Producer</c> gate
/// and the <c>ProducerWorker</c> wrap — records what was produced and flushed, and can be told to fail
/// a produce or a flush. Only the members those two use are implemented; the rest throw.
/// </summary>
internal sealed class KafkaProducerFake : IProducer<Null, byte[]>
{
    /// <summary>The (topic, message) pairs handed to <see cref="ProduceAsync(string, Message{Null, byte[]}, CancellationToken)"/>, in order.</summary>
    public List<(string Topic, Message<Null, byte[]> Message)> Produced { get; } = new List<(string Topic, Message<Null, byte[]> Message)>();

    /// <summary>The number of times <see cref="Flush(CancellationToken)"/> was called.</summary>
    public int Flushes { get; private set; }

    /// <summary>Whether the wrapped producer was disposed.</summary>
    public bool Disposed { get; private set; }

    /// <summary>An exception to fail every produce with, or <see langword="null"/> to succeed.</summary>
    public Exception? ProduceFailure { get; set; }

    /// <summary>When non-empty, <see cref="ProduceFailure"/> only throws for these topics — the partial-batch-failure seam.</summary>
    public HashSet<string> FailingTopics { get; } = new HashSet<string>();

    /// <summary>An exception to fail the flush with, or <see langword="null"/> to succeed.</summary>
    public Exception? FlushFailure { get; set; }

    /// <inheritdoc />
    public Task<DeliveryResult<Null, byte[]>> ProduceAsync(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        if (ProduceFailure is not null && (FailingTopics.Count == 0 || FailingTopics.Contains(topic)))
        {
            return Task.FromException<DeliveryResult<Null, byte[]>>(ProduceFailure);
        }

        Produced.Add((topic, message));

        return Task.FromResult(new DeliveryResult<Null, byte[]>
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(Produced.Count),
            Message = message
        });
    }

    /// <inheritdoc />
    public void Flush(CancellationToken cancellationToken = default)
    {
        Flushes++;

        if (FlushFailure is not null)
        {
            throw FlushFailure;
        }
    }

    /// <inheritdoc />
    public void Dispose() => Disposed = true;

    public Task<DeliveryResult<Null, byte[]>> ProduceAsync(TopicPartition topicPartition, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public void Produce(string topic, Message<Null, byte[]> message, Action<DeliveryReport<Null, byte[]>>? deliveryHandler = null)
        => throw new NotSupportedException();

    public void Produce(TopicPartition topicPartition, Message<Null, byte[]> message, Action<DeliveryReport<Null, byte[]>>? deliveryHandler = null)
        => throw new NotSupportedException();

    public int Poll(TimeSpan timeout) => throw new NotSupportedException();

    public int Flush(TimeSpan timeout) => throw new NotSupportedException();

    public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

    public void BeginTransaction() => throw new NotSupportedException();

    public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void CommitTransaction() => throw new NotSupportedException();

    public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void AbortTransaction() => throw new NotSupportedException();

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        => throw new NotSupportedException();

    public int AddBrokers(string brokers) => throw new NotSupportedException();

    public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

    public Handle Handle => throw new NotSupportedException();

    public string Name => nameof(KafkaProducerFake);
}
