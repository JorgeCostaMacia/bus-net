using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The bus's single outbound gate: produces an already-built message to a topic. It is the one place
/// the Kafka client is written to — the <see cref="IBus"/> facade (its Send/Publish) and the
/// consumers' error and fault handlers (retries, error and fault parking) all produce through it, so
/// every outbound byte and every produce failure is instrumented in a single spot.
/// </summary>
internal interface IProducer
{
    /// <summary>Produces an already-built message to a topic. A completed task means the broker acked; a failure throws.</summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="message">The message — body and envelope headers already prepared.</param>
    /// <param name="cancellationToken">A token to cancel the produce.</param>
    Task Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Produces a batch of already-built (topic, message) pairs. They are enqueued in order — one pair
    /// per entry, so the same topic may repeat — and awaited together. A completed task means the
    /// broker acked every message; the first failure throws.
    /// </summary>
    /// <param name="messages">The (topic, message) pairs to produce, in order.</param>
    /// <param name="cancellationToken">A token to cancel the produce.</param>
    Task Produce(IEnumerable<KeyValuePair<string, Message<Null, byte[]>>> messages, CancellationToken cancellationToken = default);
}
