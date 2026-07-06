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
    private readonly SemaphoreSlim _gate = new(1, 1);

    private global::RabbitMQ.Client.IConnection? _connection;

    /// <summary>Creates the connection wrapper over the configured factory.</summary>
    /// <param name="factory">The RabbitMQ connection factory (host, credentials, vhost, recovery).</param>
    public Connection(ConnectionFactory factory) => _factory = factory;

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

            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Closes the shared connection.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null) await _connection.DisposeAsync();

        _gate.Dispose();
    }
}
