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
    /// <returns>The broker's delivery result — the topic, partition and offset the message landed at.</returns>
    Task<DeliveryResult<Null, byte[]>> Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default);
}
