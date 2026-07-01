using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Provides the global producer configuration (connection + tuning) shared by every produced
/// message. One concrete assembles it from the connection details and tuning overrides.
/// </summary>
public interface IProducerConfiguration
{
    /// <summary>The Kafka producer configuration.</summary>
    ProducerConfig ProducerConfig { get; }
}
