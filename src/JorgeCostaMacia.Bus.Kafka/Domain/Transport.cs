using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The Kafka transport for a delivered message — the concrete <see cref="ITransport"/> a context
/// carries. Wraps the low-level Kafka consume details (headers, topic, partition, offset, leader
/// epoch, timestamp) and exposes typed header getters, so a context implementation can read the
/// envelope it stored in headers without re-deserializing the message body.
/// </summary>
public sealed record Transport : ITransport
{
    /// <summary>Message headers provided by the broker.</summary>
    public ImmutableList<IHeader> Headers { get; init; }

    /// <summary>The Kafka topic the message was consumed from.</summary>
    public string Topic { get; init; }

    /// <summary>The partition within the topic where the message was stored.</summary>
    public Partition Partition { get; init; }

    /// <summary>The offset of the message within the partition — its unique position in the log.</summary>
    public Offset Offset { get; init; }

    /// <summary>The leader epoch of the partition when the message was read, when available.</summary>
    public int? LeaderEpoch { get; init; }

    /// <summary>The timestamp Kafka assigned to the message (producer time or log-append time).</summary>
    public Timestamp Timestamp { get; init; }

    /// <summary>Initializes the transport with the broker-provided consume details.</summary>
    /// <param name="headers">The message headers.</param>
    /// <param name="topic">The topic the message was consumed from.</param>
    /// <param name="partition">The partition within the topic.</param>
    /// <param name="offset">The offset within the partition.</param>
    /// <param name="leaderEpoch">The leader epoch, when available.</param>
    /// <param name="timestamp">The Kafka timestamp.</param>
    public Transport(ImmutableList<IHeader> headers, string topic, Partition partition, Offset offset, int? leaderEpoch, Timestamp timestamp)
    {
        Headers = headers;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        LeaderEpoch = leaderEpoch;
        Timestamp = timestamp;
    }

    /// <summary>Creates the transport for a delivered message from the broker-provided consume result.</summary>
    /// <param name="result">The delivered message.</param>
    /// <returns>The delivery's transport.</returns>
    public static Transport Create(ConsumeResult<Ignore, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);

    /// <summary>
    /// Decodes every header to browsable text, order and duplicate keys preserved (Kafka allows
    /// them) — the envelope's known binary GUID headers render as GUIDs, everything else as
    /// best-effort UTF-8. The view embedded in the bodies parked to <c>.error</c> / <c>.fault</c>.
    /// </summary>
    /// <returns>Every header as a key/text pair.</returns>
    internal ImmutableList<KeyValuePair<string, string>> DecodeHeaders()
        => Headers
            .Select(header => new KeyValuePair<string, string>(header.Key, Decode(header)))
            .ToImmutableList();

    /// <summary>Decodes one header's value: a GUID for the envelope's binary GUID keys, best-effort UTF-8 otherwise.</summary>
    private static string Decode(IHeader header)
    {
        byte[] value = header.GetValueBytes();

        return TransportHeaders.GuidHeaders.Contains(header.Key) && value.Length == 16
            ? new Guid(value).ToString()
            : Encoding.UTF8.GetString(value);
    }

    /// <summary>
    /// Clones this delivery's headers, deep-copying each value's bytes, so the result can be re-stamped
    /// for an outbound message without mutating the original delivery's headers.
    /// </summary>
    /// <returns>A new header set holding independent copies of every header's key and bytes.</returns>
    public Headers CloneHeaders()
    {
        Headers clonedHeaders = new Headers();

        foreach (IHeader header in Headers)
        {
            clonedHeaders.Add(header.Key, (byte[])header.GetValueBytes().Clone());
        }

        return clonedHeaders;
    }

    /// <summary>Returns the raw bytes of the last header with the given <paramref name="key"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value as bytes.</returns>
    /// <exception cref="KeyNotFoundException">No header with <paramref name="key"/> is present.</exception>
    public byte[] GetHeader(string key)
    {
        byte[]? header = Headers.LastOrDefault(e => e.Key == key)?.GetValueBytes();

        if (header is null) throw new KeyNotFoundException($"The key '{key}' was not present in the headers collection.");

        return header;
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
    public string GetHeaderString(string key)
    {
        byte[] header = GetHeader(key);

        return Encoding.UTF8.GetString(header);
    }

    /// <summary>
    /// Reads the header with the given <paramref name="key"/> as a UTF-8 string, or
    /// <see langword="null"/> when the header is absent — for optional envelope fields (e.g. the
    /// origin address, which a flow's first message does not carry).
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded as UTF-8, or <see langword="null"/> when absent.</returns>
    public string? GetHeaderStringOrDefault(string key)
    {
        byte[]? header = Headers.LastOrDefault(e => e.Key == key)?.GetValueBytes();

        return header is null ? null : Encoding.UTF8.GetString(header);
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTC <see cref="DateTime"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a UTC <see cref="DateTime"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a valid date/time.</exception>
    public DateTime GetHeaderDateTime(string key)
    {
        string header = GetHeaderString(key);

        if (DateTime.TryParse(header, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTime value))
        {
            return value;
        }

        throw new InvalidCastException($"The key '{key}' was not a valid DateTime.");
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a comma-separated string list.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The trimmed, non-empty comma-separated values.</returns>
    public ImmutableList<string> GetHeaderStringList(string key)
    {
        string header = GetHeaderString(key);

        return header
            .Split(',')
            .Select(e => e.Trim())
            .Where(e => !string.IsNullOrWhiteSpace(e))
            .ToImmutableList();
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as an <see cref="int"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as an <see cref="int"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a valid integer.</exception>
    public int GetHeaderInt(string key)
    {
        string header = GetHeaderString(key);

        if (int.TryParse(header, out int value)) return value;

        throw new InvalidCastException($"The key '{key}' was not a valid int.");
    }
}
