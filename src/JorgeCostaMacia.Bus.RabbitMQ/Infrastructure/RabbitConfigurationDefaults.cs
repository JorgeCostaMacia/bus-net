namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// Default connection settings a <see cref="RabbitConfiguration"/> falls back to for values the
/// <c>Bus:Connection</c> section does not supply.
/// </summary>
public static class RabbitConfigurationDefaults
{
    /// <summary>AMQP port. Default: <c>5672</c>.</summary>
    public const int PORT = 5672;

    /// <summary>Virtual host. Default: <c>/</c>.</summary>
    public const string VIRTUAL_HOST = "/";

    /// <summary>Automatic connection/topology recovery. Default: <c>true</c> — the connection self-heals after a drop.</summary>
    public const bool AUTOMATIC_RECOVERY_ENABLED = true;

    /// <summary>Client-provided connection name (shown in the RabbitMQ management UI). Default: the machine name.</summary>
    public static string CLIENT_PROVIDED_NAME => Environment.MachineName;
}
