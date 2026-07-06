using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The consume side's inbound gate — the seam over the Kafka client a <c>ConsumerWorker</c> reads
/// through, the mirror of the <see cref="IProducer"/> gate on the send side. One per worker: each
/// handler has its own group, topic and loop, so — unlike the shared producer — these are not
/// singletons. Exposes only what the loop needs (subscribe, consume, store the offset which is the
/// ack, and close), so the delivery flow can be driven over a fake without a live broker.
/// </summary>
internal interface IConsumer : IDisposable
{
    /// <summary>Subscribes to the topic the worker consumes.</summary>
    /// <param name="topic">The topic to subscribe to.</param>
    void Subscribe(string topic);

    /// <summary>Blocks until the next message is delivered or the token is cancelled.</summary>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The delivered message.</returns>
    ConsumeResult<Ignore, byte[]> Consume(CancellationToken cancellationToken);

    /// <summary>Stores the delivery's offset — the ack; the client commits it in the background without blocking.</summary>
    /// <param name="result">The delivery whose offset is stored.</param>
    void StoreOffset(ConsumeResult<Ignore, byte[]> result);

    /// <summary>Closes the consumer — final offsets committed, group left per its membership.</summary>
    void Close();
}
