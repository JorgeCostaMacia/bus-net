using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The global Kafka configuration shared by every message: the producer and admin-client settings
/// (connection + tuning). Per-message topic config lives in <see cref="IMessageConfiguration"/>
/// (and per-handler consumer config in the handler configuration) — both held by the bus, not here —
/// so this stays focused on the connection-level settings and never grows into a mega-object.
/// </summary>
public interface IBusConfiguration
{
    /// <summary>The shared producer configuration (connection + tuning) used for every message.</summary>
    ProducerConfig ProducerConfig { get; }

    /// <summary>The shared admin-client configuration (connection) used for topic provisioning.</summary>
    AdminClientConfig AdminClientConfig { get; }
}
