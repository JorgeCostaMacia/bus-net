namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>Default topic settings applied to an <see cref="EventConfiguration{TEvent}"/> when not supplied.</summary>
public static class EventConfigurationDefaults
{
    /// <summary>
    /// Number of partitions per Kafka topic. Default: <c>6</c>.
    /// <see href="https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html"/>
    /// </summary>
    public const int NUM_PARTITIONS = 6;

    /// <summary>
    /// Replication factor per Kafka topic. Default: <c>3</c>.
    /// <see href="https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html"/>
    /// </summary>
    public const short REPLICATION_FACTOR = 3;
}
