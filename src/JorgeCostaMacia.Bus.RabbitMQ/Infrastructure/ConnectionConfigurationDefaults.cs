namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// Default connection settings a <see cref="ConnectionConfiguration"/> falls back to for values the
/// <c>Bus:Connection</c> section does not supply.
/// </summary>
public static class ConnectionConfigurationDefaults
{
    /// <summary>
    /// Whether TLS wraps the connection. Default: <c>true</c> — secure by default, matching the
    /// Kafka transport's SaslSsl default; the AMQPS port follows unless one is supplied.
    /// </summary>
    public const bool SSL = true;

    /// <summary>AMQP port used when TLS is off and no port is supplied. Default: <c>5672</c>.</summary>
    public const int PORT = 5672;

    /// <summary>AMQPS port used when TLS is on and no port is supplied. Default: <c>5671</c>.</summary>
    public const int SSL_PORT = 5671;

    /// <summary>Virtual host. Default: <c>/</c>.</summary>
    public const string VIRTUAL_HOST = "/";

    /// <summary>Automatic connection/topology recovery. Default: <c>true</c> — the connection self-heals after a drop.</summary>
    public const bool AUTOMATIC_RECOVERY_ENABLED = true;

    /// <summary>Client-provided connection name (shown in the RabbitMQ management UI). Default: the machine name.</summary>
    public static string CLIENT_PROVIDED_NAME => Environment.MachineName;
}
