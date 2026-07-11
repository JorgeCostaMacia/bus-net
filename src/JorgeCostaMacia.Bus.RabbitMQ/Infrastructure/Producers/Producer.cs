using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The container-owned outbound gate: publishes through one long-lived, confirmation-enabled
/// <see cref="IChannel"/> <b>per destination exchange</b> — opened lazily on the first publish to
/// that exchange from the shared <see cref="Domain.IConnection"/>, reused for the application's
/// lifetime, and replaced under the gate when the broker closes it. Concurrent publishes share the
/// destination's channel safely: the client pipelines them and tracks each confirmation, throttling
/// the outstanding ones on its own — so the channel count is bounded by the routing map, never by
/// the load. Being the one place every outbound byte flows through, it stamps the producing host's
/// <c>jcm-host-*</c> headers on each message (so retries and error/fault parking carry them too),
/// and mirrors the envelope's key ids onto the native AMQP <see cref="BasicProperties"/> (message
/// id, correlation, type, timestamp, app, content type) so RabbitMQ tooling and other clients see
/// them without decoding the <c>jcm-*</c> headers — which stay the source of truth the contexts read.
/// </summary>
internal sealed class Producer : Domain.IProducer, IAsyncDisposable
{
    private static readonly IReadOnlyList<KeyValuePair<string, object?>> HOST = Host();

    private readonly Domain.IConnection _connection;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ConcurrentDictionary<string, IChannel> _channels = new();

    /// <summary>Creates the producer over the shared connection.</summary>
    /// <param name="connection">The shared RabbitMQ connection the destination channels are opened on.</param>
    public Producer(Domain.IConnection connection) => _connection = connection;

    /// <inheritdoc />
    public async Task Produce(string exchange, string routingKey, ReadOnlyMemory<byte> body, IReadOnlyDictionary<string, object?> headers, CancellationToken cancellationToken = default)
    {
        IChannel channel = await ChannelAsync(exchange, cancellationToken);

        Dictionary<string, object?> stamped = new(headers);

        foreach (KeyValuePair<string, object?> host in HOST) stamped[host.Key] = host.Value;

        BasicProperties properties = new()
        {
            Persistent = true,
            ContentType = "application/json",
            Headers = stamped
        };

        Native(properties, stamped);

        await channel.BasicPublishAsync(exchange, routingKey, mandatory: false, basicProperties: properties, body: body, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Mirrors the envelope's key ids from the <c>jcm-*</c> headers onto the native AMQP properties, so
    /// RabbitMQ tooling and other clients read them without decoding headers. The headers stay the
    /// source of truth; this is a write-only convenience on produce. A missing/undecodable header just
    /// leaves its property unset.
    /// </summary>
    private static void Native(BasicProperties properties, IReadOnlyDictionary<string, object?> headers)
    {
        if (GuidText(headers, TransportHeaders.MessageId) is { } messageId) properties.MessageId = messageId;
        if (GuidText(headers, TransportHeaders.ConversationId) is { } correlationId) properties.CorrelationId = correlationId;
        if (Text(headers, TransportHeaders.MessageType) is { } type) properties.Type = type;
        if (Text(headers, TransportHeaders.HostAssembly) is { } appId) properties.AppId = appId;

        if (Text(headers, TransportHeaders.MessageOccurredAt) is { } occurredAt
            && DateTimeOffset.TryParse(occurredAt, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTimeOffset moment))
        {
            properties.Timestamp = new AmqpTimestamp(moment.ToUnixTimeSeconds());
        }
    }

    /// <summary>Reads a header as UTF-8 text, or <see langword="null"/> when absent (or not byte-valued).</summary>
    private static string? Text(IReadOnlyDictionary<string, object?> headers, string key)
        => headers.TryGetValue(key, out object? value) && value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : null;

    /// <summary>Reads a 16-byte GUID header as its canonical text, or <see langword="null"/> when absent (or not a 16-byte value).</summary>
    private static string? GuidText(IReadOnlyDictionary<string, object?> headers, string key)
        => headers.TryGetValue(key, out object? value) && value is byte[] bytes && bytes.Length == 16 ? new Guid(bytes).ToString() : null;

    /// <summary>
    /// The destination's channel — opened on the first publish to the exchange and reused for the
    /// application's lifetime; a channel the broker has closed (e.g. an async publish error) is
    /// replaced under the gate instead of handed out dead.
    /// </summary>
    private async Task<IChannel> ChannelAsync(string exchange, CancellationToken cancellationToken)
    {
        if (_channels.TryGetValue(exchange, out IChannel? channel) && channel.IsOpen) return channel;

        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (_channels.TryGetValue(exchange, out channel) && channel.IsOpen) return channel;

            if (channel is not null) await channel.DisposeAsync();

            channel = await _connection.CreateChannelAsync(cancellationToken);

            _channels[exchange] = channel;

            return channel;
        }
        finally
        {
            _gate.Release();
        }
    }

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

    /// <summary>Closes every destination channel — the container disposes the singleton at shutdown.</summary>
    public async ValueTask DisposeAsync()
    {
        foreach (IChannel channel in _channels.Values) await channel.DisposeAsync();
    }
}
