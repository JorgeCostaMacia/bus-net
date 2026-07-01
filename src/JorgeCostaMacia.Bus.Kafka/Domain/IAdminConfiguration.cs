using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Provides the global admin-client configuration (connection) used for topic provisioning.
/// </summary>
public interface IAdminConfiguration
{
    /// <summary>The Kafka admin-client configuration.</summary>
    AdminClientConfig AdminClientConfig { get; }
}
