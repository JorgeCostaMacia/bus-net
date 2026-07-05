using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// In-memory inbound gate driving the consume loop deterministically: it hands out a fixed queue of
/// deliveries and then, once drained, blocks until the loop's token is cancelled (mirroring a real
/// consumer waiting for the next message). <see cref="Drained"/> completes when the loop asks for a
/// message after the queue is empty — which only happens once every fed delivery has been fully
/// processed (consumed, handled and its offset decided), so a test can await it, stop the worker and
/// assert. Records the stored offsets (the acks) and the close/dispose.
/// </summary>
internal sealed class ConsumerFake : IConsumer
{
    private readonly Queue<ConsumeResult<Ignore, byte[]>> _pending;
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ManualResetEventSlim _park = new(false);

    /// <summary>Creates the fake over the deliveries the loop will consume, in order.</summary>
    /// <param name="deliveries">The deliveries handed to the loop, oldest first.</param>
    public ConsumerFake(params ConsumeResult<Ignore, byte[]>[] deliveries) => _pending = new Queue<ConsumeResult<Ignore, byte[]>>(deliveries);

    /// <summary>The offsets stored (acked) by the loop, in order.</summary>
    public List<TopicPartitionOffset> Stored { get; } = [];

    /// <summary>The topic the loop subscribed to.</summary>
    public string? SubscribedTopic { get; private set; }

    /// <summary>Whether the loop closed the consumer on stop.</summary>
    public bool Closed { get; private set; }

    /// <summary>Whether the loop disposed the consumer on stop.</summary>
    public bool Disposed { get; private set; }

    /// <summary>Completes once every fed delivery has been consumed and fully processed.</summary>
    public Task Drained => _drained.Task;

    /// <inheritdoc />
    public void Subscribe(string topic) => SubscribedTopic = topic;

    /// <inheritdoc />
    public ConsumeResult<Ignore, byte[]> Consume(CancellationToken cancellationToken)
    {
        if (_pending.Count > 0) return _pending.Dequeue();

        _drained.TrySetResult();
        _park.Wait(cancellationToken);

        throw new OperationCanceledException(cancellationToken);
    }

    /// <inheritdoc />
    public void StoreOffset(ConsumeResult<Ignore, byte[]> result) => Stored.Add(result.TopicPartitionOffset);

    /// <inheritdoc />
    public void Close() => Closed = true;

    /// <inheritdoc />
    public void Dispose() => Disposed = true;
}
