namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Maps a message type to its Kafka topic — the bus's routing entry. Topics themselves are
/// infrastructure: the broker auto-creates them on first use with its defaults, and they are managed
/// (partitions, replicas, configs) broker-side, not by the bus.
/// </summary>
public interface IMessageConfiguration
{
    /// <summary>The CLR type of the message.</summary>
    Type MessageType { get; }

    /// <summary>The Kafka topic the message is routed to.</summary>
    string Topic { get; }
}
