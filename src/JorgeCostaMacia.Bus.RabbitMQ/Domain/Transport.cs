using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using JorgeCostaMacia.Bus.Domain;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The RabbitMQ transport for a delivered message — the concrete <see cref="ITransport"/> a context
/// carries. Wraps the low-level AMQP delivery details (headers, exchange, routing key, delivery tag,
/// redelivered flag) and exposes typed header getters, so a context implementation can read the
/// envelope it stored in headers without re-deserializing the message body. Header values travel as
/// raw bytes (RabbitMQ returns field-table strings as bytes), decoded the same way as on Kafka.
/// </summary>
public sealed record Transport : ITransport
{
    /// <summary>Message headers from the AMQP field table.</summary>
    public IReadOnlyDictionary<string, object?> Headers { get; init; }

    /// <summary>The exchange the message was published to.</summary>
    public string Exchange { get; init; }

    /// <summary>The routing key the message was published with.</summary>
    public string RoutingKey { get; init; }

    /// <summary>The broker-assigned delivery tag — the channel-scoped id used to ack/nack this delivery.</summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>Whether the broker redelivered this message (a prior delivery was not acked).</summary>
    public bool Redelivered { get; init; }

    /// <summary>Initializes the transport with the broker-provided delivery details.</summary>
    /// <param name="headers">The message headers.</param>
    /// <param name="exchange">The exchange the message was published to.</param>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="deliveryTag">The delivery tag used to ack/nack.</param>
    /// <param name="redelivered">Whether the message was redelivered.</param>
    public Transport(IReadOnlyDictionary<string, object?> headers, string exchange, string routingKey, ulong deliveryTag, bool redelivered)
    {
        Headers = headers;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
    }

    /// <summary>Creates the transport for a delivered message from the consumer's delivery args.</summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>The delivery's transport.</returns>
    public static Transport Create(BasicDeliverEventArgs args)
        => new(
            args.BasicProperties.Headers is { } headers ? new Dictionary<string, object?>(headers) : [],
            args.Exchange,
            args.RoutingKey,
            args.DeliveryTag,
            args.Redelivered);

    /// <summary>
    /// Decodes every header to browsable text — the envelope's known binary GUID headers render as
    /// GUIDs, everything else as best-effort UTF-8. The view embedded in the bodies parked to error.
    /// </summary>
    /// <returns>Every header as a key/text pair.</returns>
    internal ImmutableList<KeyValuePair<string, string>> DecodeHeaders()
        => Headers
            .Select(header => new KeyValuePair<string, string>(header.Key, Decode(header.Key, header.Value)))
            .ToImmutableList();

    /// <summary>Decodes one header's value: a GUID for the envelope's binary GUID keys, best-effort UTF-8 otherwise.</summary>
    private static string Decode(string key, object? value)
    {
        if (value is not byte[] bytes) return value?.ToString() ?? string.Empty;

        return TransportHeaders.GuidHeaders.Contains(key) && bytes.Length == 16
            ? new Guid(bytes).ToString()
            : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Clones this delivery's headers, deep-copying each value's bytes, so the result can be re-stamped for an outbound message without mutating the original.</summary>
    /// <returns>A new header table holding independent copies of every header's bytes.</returns>
    public Dictionary<string, object?> CloneHeaders()
    {
        Dictionary<string, object?> clonedHeaders = [];

        foreach ((string key, object? value) in Headers)
        {
            clonedHeaders[key] = value is byte[] bytes ? (byte[])bytes.Clone() : value;
        }

        return clonedHeaders;
    }

    /// <summary>Returns the raw bytes of the header with the given <paramref name="key"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value as bytes.</returns>
    /// <exception cref="KeyNotFoundException">No header with <paramref name="key"/> is present (or it is not byte-valued).</exception>
    public byte[] GetHeader(string key)
    {
        if (!Headers.TryGetValue(key, out object? value) || value is not byte[] bytes) throw new KeyNotFoundException($"The key '{key}' was not present in the headers collection.");

        return bytes;
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a <see cref="Guid"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a <see cref="Guid"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a 16-byte GUID.</exception>
    public Guid GetHeaderGuid(string key)
    {
        byte[] header = GetHeader(key);

        if (header.Length != 16) throw new InvalidCastException($"The key '{key}' was not a valid Guid.");

        return new Guid(header);
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTF-8 string.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded as UTF-8.</returns>
    public string GetHeaderString(string key) => Encoding.UTF8.GetString(GetHeader(key));

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTF-8 string, or <see langword="null"/> when the header is absent — for optional envelope fields.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded as UTF-8, or <see langword="null"/> when absent.</returns>
    public string? GetHeaderStringOrDefault(string key)
        => Headers.TryGetValue(key, out object? value) && value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : null;

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTC <see cref="DateTime"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a UTC <see cref="DateTime"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a valid date/time.</exception>
    public DateTime GetHeaderDateTime(string key)
    {
        string header = GetHeaderString(key);

        if (DateTime.TryParse(header, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTime value)) return value;

        throw new InvalidCastException($"The key '{key}' was not a valid DateTime.");
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a comma-separated string list.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The trimmed, non-empty comma-separated values.</returns>
    public ImmutableList<string> GetHeaderStringList(string key)
        => GetHeaderString(key)
            .Split(',')
            .Select(value => value.Trim())
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .ToImmutableList();

    /// <summary>Reads the header with the given <paramref name="key"/> as an <see cref="int"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as an <see cref="int"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a valid integer.</exception>
    public int GetHeaderInt(string key)
    {
        string header = GetHeaderString(key);

        if (int.TryParse(header, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value)) return value;

        throw new InvalidCastException($"The key '{key}' was not a valid int.");
    }
}
