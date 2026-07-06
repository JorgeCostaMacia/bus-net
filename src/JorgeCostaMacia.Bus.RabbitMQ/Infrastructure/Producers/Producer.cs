using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The scoped outbound gate: publishes through a single <see cref="IChannel"/> it opens lazily on
/// first use from the shared <see cref="Domain.IConnection"/> and reuses for the scope's lifetime,
/// disposing it when the scope ends. Scoped — not a singleton — because a channel is not safe for
/// concurrent publish; one producer per scope is used single-threaded.
/// </summary>
internal sealed class Producer : Domain.IProducer, IAsyncDisposable
{
    private readonly Domain.IConnection _connection;

    private IChannel? _channel;

    /// <summary>Creates the producer over the shared connection.</summary>
    /// <param name="connection">The shared RabbitMQ connection the channel is opened on.</param>
    public Producer(Domain.IConnection connection) => _connection = connection;

    /// <inheritdoc />
    public async Task Produce(string exchange, string routingKey, ReadOnlyMemory<byte> body, IReadOnlyDictionary<string, object?> headers, CancellationToken cancellationToken = default)
    {
        IChannel channel = await ChannelAsync(cancellationToken);

        BasicProperties properties = new()
        {
            Persistent = true,
            Headers = headers.ToDictionary(header => header.Key, header => header.Value)
        };

        await channel.BasicPublishAsync(exchange, routingKey, mandatory: false, basicProperties: properties, body: body, cancellationToken: cancellationToken);
    }

    /// <summary>The scope's channel — opened once on first publish, reused thereafter.</summary>
    private async Task<IChannel> ChannelAsync(CancellationToken cancellationToken)
        => _channel ??= await _connection.CreateChannelAsync(cancellationToken);

    /// <summary>Closes the scope's channel.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_channel is not null) await _channel.DisposeAsync();
    }
}
