using System.Reflection;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The scoped outbound gate: publishes through a single <see cref="IChannel"/> it opens lazily on
/// first use from the shared <see cref="Domain.IConnection"/> and reuses for the scope's lifetime,
/// disposing it when the scope ends. Scoped — not a singleton — because a channel is not safe for
/// concurrent publish; one producer per scope is used single-threaded. Being the one place every
/// outbound byte flows through, it stamps the producing host's <c>jcm_host_*</c> headers on each
/// message (so retries and error/fault parking carry them too).
/// </summary>
internal sealed class Producer : Domain.IProducer, IAsyncDisposable
{
    private static readonly IReadOnlyList<KeyValuePair<string, object?>> HOST = Host();

    private readonly Domain.IConnection _connection;

    private IChannel? _channel;

    /// <summary>Creates the producer over the shared connection.</summary>
    /// <param name="connection">The shared RabbitMQ connection the channel is opened on.</param>
    public Producer(Domain.IConnection connection) => _connection = connection;

    /// <inheritdoc />
    public async Task Produce(string exchange, string routingKey, ReadOnlyMemory<byte> body, IReadOnlyDictionary<string, object?> headers, CancellationToken cancellationToken = default)
    {
        IChannel channel = await ChannelAsync(cancellationToken);

        Dictionary<string, object?> stamped = new(headers);

        foreach (KeyValuePair<string, object?> host in HOST) stamped[host.Key] = host.Value;

        BasicProperties properties = new()
        {
            Persistent = true,
            Headers = stamped
        };

        await channel.BasicPublishAsync(exchange, routingKey, mandatory: false, basicProperties: properties, body: body, cancellationToken: cancellationToken);
    }

    /// <summary>The scope's channel — opened once on first publish, reused thereafter.</summary>
    private async Task<IChannel> ChannelAsync(CancellationToken cancellationToken)
        => _channel ??= await _connection.CreateChannelAsync(cancellationToken);

    /// <summary>The producing host's identity, captured once as ready-to-stamp header bytes.</summary>
    private static IReadOnlyList<KeyValuePair<string, object?>> Host()
    {
        AssemblyName? entry = Assembly.GetEntryAssembly()?.GetName();

        return
        [
            new(TransportHeaders.HostMachineName, TransportHeaders.ToHeader(Environment.MachineName)),
            new(TransportHeaders.HostAssembly, TransportHeaders.ToHeader(entry?.Name ?? "unknown")),
            new(TransportHeaders.HostAssemblyVersion, TransportHeaders.ToHeader(entry?.Version?.ToString() ?? "0.0.0")),
            new(TransportHeaders.HostFrameworkVersion, TransportHeaders.ToHeader(Environment.Version.ToString())),
            new(TransportHeaders.HostBusVersion, TransportHeaders.ToHeader(typeof(Producer).Assembly.GetName().Version?.ToString() ?? "0.0.0")),
            new(TransportHeaders.HostOperatingSystemVersion, TransportHeaders.ToHeader(Environment.OSVersion.ToString()))
        ];
    }

    /// <summary>Closes the scope's channel.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_channel is not null) await _channel.DisposeAsync();
    }
}
