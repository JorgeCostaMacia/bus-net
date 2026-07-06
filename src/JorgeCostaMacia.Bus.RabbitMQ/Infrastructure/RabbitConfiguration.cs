using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The connection configuration, bound from the <c>Bus:Connection</c> section: the single RabbitMQ
/// connection shared by the producing and consuming sides. Unset values fall back to
/// <see cref="RabbitConfigurationDefaults"/> when composing the <see cref="ConnectionFactory"/>.
/// </summary>
public sealed record RabbitConfiguration
{
    /// <summary>The RabbitMQ host name. Required.</summary>
    public required string HostName { get; init; }

    /// <summary>The user name. Required.</summary>
    public required string UserName { get; init; }

    /// <summary>The password. Required.</summary>
    public required string Password { get; init; }

    /// <summary>The AMQP port, or <see langword="null"/> for the default (5672).</summary>
    public int? Port { get; init; }

    /// <summary>The virtual host, or <see langword="null"/> for the default (<c>/</c>).</summary>
    public string? VirtualHost { get; init; }

    /// <summary>The client-provided connection name, or <see langword="null"/> for the default (machine name).</summary>
    public string? ClientProvidedName { get; init; }

    /// <summary>Automatic recovery, or <see langword="null"/> for the default (true).</summary>
    public bool? AutomaticRecoveryEnabled { get; init; }

    /// <summary>The RabbitMQ connection factory — supplied values, defaults for the rest.</summary>
    public ConnectionFactory ConnectionFactory => new()
    {
        HostName = HostName,
        UserName = UserName,
        Password = Password,
        Port = Port ?? RabbitConfigurationDefaults.PORT,
        VirtualHost = VirtualHost ?? RabbitConfigurationDefaults.VIRTUAL_HOST,
        ClientProvidedName = ClientProvidedName ?? RabbitConfigurationDefaults.CLIENT_PROVIDED_NAME,
        AutomaticRecoveryEnabled = AutomaticRecoveryEnabled ?? RabbitConfigurationDefaults.AUTOMATIC_RECOVERY_ENABLED
    };
}
