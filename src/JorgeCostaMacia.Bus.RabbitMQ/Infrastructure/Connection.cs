using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The container-owned singleton wrapping the RabbitMQ connection: opens it lazily on first use from
/// the configured <see cref="ConnectionFactory"/> (with automatic recovery), guards the open with a
/// gate so concurrent callers share one connection, and re-opens it if it has dropped. Hands out a
/// fresh channel per request — the connection is shared and thread-safe, the channels are not.
/// </summary>
internal sealed class Connection : Domain.IConnection
{
    private readonly ConnectionFactory _factory;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private global::RabbitMQ.Client.IConnection? _connection;

    /// <summary>Creates the connection wrapper over the configured factory.</summary>
    /// <param name="factory">The RabbitMQ connection factory (host, credentials, vhost, recovery).</param>
    /// <param name="logger">The client logger — connection shutdown/recovery/callback events log through it.</param>
    public Connection(ConnectionFactory factory, ILogger logger)
    {
        _factory = factory;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        global::RabbitMQ.Client.IConnection connection = await OpenAsync(cancellationToken);

        return await connection.CreateChannelAsync(cancellationToken: cancellationToken);
    }

    /// <summary>Returns the open connection, opening (or re-opening) it under the gate when needed.</summary>
    private async Task<global::RabbitMQ.Client.IConnection> OpenAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true }) return _connection;

        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (_connection is { IsOpen: true }) return _connection;

            if (_connection is not null) await _connection.DisposeAsync();

            _connection = await _factory.CreateConnectionAsync(cancellationToken);

            Wire(_connection);

            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Routes the connection's client callbacks (shutdown, automatic recovery, callback exceptions) to the client logger.</summary>
    private void Wire(global::RabbitMQ.Client.IConnection connection)
    {
        connection.ConnectionShutdownAsync += (_, args) =>
        {
            RabbitLogger.LogShutdown(_logger, args.ReplyCode, args.ReplyText, args.Initiator == ShutdownInitiator.Application);

            return Task.CompletedTask;
        };

        connection.RecoverySucceededAsync += (_, _) =>
        {
            RabbitLogger.LogRecovered(_logger);

            return Task.CompletedTask;
        };

        connection.ConnectionRecoveryErrorAsync += (_, args) =>
        {
            RabbitLogger.LogRecoveryError(_logger, args.Exception);

            return Task.CompletedTask;
        };

        connection.CallbackExceptionAsync += (_, args) =>
        {
            RabbitLogger.LogCallbackException(_logger, args.Exception);

            return Task.CompletedTask;
        };
    }

    /// <summary>Closes the shared connection.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null) await _connection.DisposeAsync();

        _gate.Dispose();
    }
}
