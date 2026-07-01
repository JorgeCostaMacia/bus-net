using Confluent.Kafka.Admin;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Per-message Kafka topic configuration (the producer / provisioning side): the message type, its
/// topic, and the topic's partition/replication spec. The bus projects the routing map
/// (type → topic) from these, and topic provisioning uses the <see cref="TopicSpecification"/>.
/// Producer/admin connection and tuning are global (shared), not here.
/// </summary>
public interface IMessageConfiguration
{
    /// <summary>The CLR type of the message this configuration applies to.</summary>
    Type MessageType { get; }

    /// <summary>The Kafka topic the message is produced to / consumed from.</summary>
    string Topic { get; }

    /// <summary>Number of partitions for the topic.</summary>
    int NumPartitions { get; }

    /// <summary>Replication factor for the topic.</summary>
    short ReplicationFactor { get; }

    /// <summary>The Kafka topic specification, for topic creation / validation.</summary>
    TopicSpecification TopicSpecification { get; }
}
