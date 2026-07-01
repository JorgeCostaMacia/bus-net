using Confluent.Kafka.Admin;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka topic configuration for a command type. Feeds routing (type → topic) and topic
/// provisioning through its <see cref="TopicSpecification"/>. The producer connection and tuning are
/// global (shared), not here.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public sealed record CommandConfiguration<TCommand> : IMessageConfiguration
    where TCommand : ICommand
{
    /// <summary>The CLR type of the command.</summary>
    public Type MessageType { get; init; }

    /// <summary>The Kafka topic specification (name / partitions / replication).</summary>
    public TopicSpecification TopicSpecification { get; init; }

    /// <summary>Configures the command's topic; partitions/replication fall back to the defaults.</summary>
    /// <param name="topic">The Kafka topic.</param>
    /// <param name="numPartitions">Partitions, or <see langword="null"/> for the default.</param>
    /// <param name="replicationFactor">Replication factor, or <see langword="null"/> for the default.</param>
    public CommandConfiguration(string topic, int? numPartitions = null, short? replicationFactor = null)
    {
        MessageType = typeof(TCommand);
        TopicSpecification = new()
        {
            Name = topic,
            NumPartitions = numPartitions ?? CommandConfigurationDefaults.NUM_PARTITIONS,
            ReplicationFactor = replicationFactor ?? CommandConfigurationDefaults.REPLICATION_FACTOR
        };
    }
}
