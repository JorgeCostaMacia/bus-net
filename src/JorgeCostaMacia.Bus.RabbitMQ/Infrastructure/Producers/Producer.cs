using System.Globalization;
using System.Reflection;
using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The scoped outbound gate: publishes through a single <see cref="IChannel"/> it opens lazily on
/// first use from the shared <see cref="Domain.IConnection"/> and reuses for the scope's lifetime,
/// disposing it when the scope ends. Scoped — not a singleton — because a channel is not safe for
/// concurrent publish; one producer per scope is used single-threaded. Being the one place every
/// outbound byte flows through, it stamps the producing host's <c>jcm-host-*</c> headers on each
/// message (so retries and error/fault parking carry them too), and mirrors the envelope's key ids
/// onto the native AMQP <see cref="BasicProperties"/> (message id, correlation, type, timestamp, app,
/// content type) so RabbitMQ tooling and other clients see them without decoding the <c>jcm-*</c>
/// headers — which stay the source of truth the contexts read.
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
