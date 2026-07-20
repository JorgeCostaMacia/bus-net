using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;

/// <summary>
/// The topic-provisioning configuration, bound from the <c>Bus:Admin</c> section: the connection and
/// credentials the admin client uses to create the declared topics at startup. Kept separate from
/// <c>Bus:Producer</c>/<c>Bus:Consumer</c> so the create-topics permission can live on a dedicated admin
/// user while the runtime produce/consume user stays least-privileged. Unset security settings fall back
/// to the producer defaults when composing the <see cref="AdminClientConfig"/>.
/// </summary>
public sealed record AdminConfiguration
{
    /// <summary>Comma-separated list of Kafka brokers. Required.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>SASL username — the admin user with the create-topics permission. Required.</summary>
    public required string SaslUsername { get; init; }

    /// <summary>SASL password. Required.</summary>
    public required string SaslPassword { get; init; }

    /// <summary>Security protocol, or <see langword="null"/> for the default.</summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>SASL mechanism, or <see langword="null"/> for the default.</summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>The Kafka admin client configuration — supplied values, producer defaults for the rest.</summary>
    public AdminClientConfig AdminClientConfig => new AdminClientConfig()
    {
        BootstrapServers = BootstrapServers,
        SecurityProtocol = SecurityProtocol ?? ProducerConfigurationDefaults.SecurityProtocol,
        SaslMechanism = SaslMechanism ?? ProducerConfigurationDefaults.SaslMechanism,
        SaslUsername = SaslUsername,
        SaslPassword = SaslPassword
    };
}
