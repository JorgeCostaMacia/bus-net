using Confluent.Kafka.Admin;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka topic configuration for an event type. Feeds routing (type → topic) and topic
/// provisioning through its <see cref="TopicSpecification"/>. The producer connection and tuning are
/// global (shared), not here.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
public sealed record EventConfiguration<TEvent> : IMessageConfiguration
    where TEvent : IEvent
{
    /// <summary>The CLR type of the event.</summary>
    public Type MessageType { get; init; }

    /// <summary>The Kafka topic specification (name / partitions / replication).</summary>
    public TopicSpecification TopicSpecification { get; init; }

    /// <summary>Configures the event's topic; partitions/replication fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic.</param>
    /// <param name="numPartitions">Partitions, or <see langword="null"/> for the default.</param>
    /// <param name="replicationFactor">Replication factor, or <see langword="null"/> for the default.</param>
    public EventConfiguration(string topic, int? numPartitions = null, short? replicationFactor = null)
    {
        MessageType = typeof(TEvent);
        TopicSpecification = new()
        {
            Name = topic,
            NumPartitions = numPartitions ?? EventConfigurationDefaults.NUM_PARTITIONS,
            ReplicationFactor = replicationFactor ?? EventConfigurationDefaults.REPLICATION_FACTOR
        };
    }
}
