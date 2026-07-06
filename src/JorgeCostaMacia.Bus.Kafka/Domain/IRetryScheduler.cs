using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Schedules a delayed retry: the parked delivery is produced back to its original topic at the
/// given time — the envelope already carries the incremented retry count and the consumer targeting,
/// so the normal consumers receive and route it with no extra machinery. Implementations own the
/// parking mechanism (e.g. a Quartz one-shot job); registering one enables the positive intervals of
/// the retry ladder — without it a positive interval cannot be delayed, so it is parked to the topic's
/// <c>.error</c> as terminal.
/// </summary>
public interface IRetryScheduler
{
    /// <summary>Parks a delivery to be produced back to its topic at the given time.</summary>
    /// <param name="topic">The original Kafka topic to produce back to.</param>
    /// <param name="groupId">The failing consumer group id — carried for traceability (e.g. the parked retry's description).</param>
    /// <param name="body">The raw message body.</param>
    /// <param name="headers">The envelope to travel with the retry — retry count and targeting already stamped.</param>
    /// <param name="scheduledAt">The UTC time to produce the retry at.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Schedule(string topic, string groupId, byte[] body, Headers headers, DateTime scheduledAt, CancellationToken cancellationToken);
}
