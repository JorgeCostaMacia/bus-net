using Confluent.Kafka.Admin;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Per-message Kafka topic configuration: the message type and its topic specification (name,
/// partitions, replication). The bus projects the routing map (type → topic) from the spec's name,
/// and topic provisioning uses the spec. Producer/admin connection and tuning are global (shared),
/// not here.
/// </summary>
public interface IMessageConfiguration
{
    /// <summary>The CLR type of the message this configuration applies to.</summary>
    Type MessageType { get; }

    /// <summary>The Kafka topic specification (name / partitions / replication) — routing and topic creation.</summary>
    TopicSpecification TopicSpecification { get; }
}
