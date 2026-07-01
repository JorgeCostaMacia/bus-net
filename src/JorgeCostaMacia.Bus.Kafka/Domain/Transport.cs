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
    public Guid GetGuid(string key)
    {
        byte[] header = GetHeader(key);

        if (header.Length != 16) throw new InvalidCastException($"The key '{key}' was not a valid Guid.");

        return new Guid(header);
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTF-8 string.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded as UTF-8.</returns>
    public string GetString(string key)
    {
        byte[] header = GetHeader(key);

        return Encoding.UTF8.GetString(header);
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTC <see cref="DateTime"/>.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a UTC <see cref="DateTime"/>.</returns>
    /// <exception cref="InvalidCastException">The header value is not a valid date/time.</exception>
    public DateTime GetDateTime(string key)
    {
        string header = GetString(key);

        if (DateTime.TryParse(header, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTime value))
        {
            return value;
        }

        throw new InvalidCastException($"The key '{key}' was not a valid DateTime.");
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a comma-separated string list.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The trimmed, non-empty comma-separated values.</returns>
    public ImmutableList<string> GetStringList(string key)
    {
        string header = GetString(key);

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
    public int GetInt(string key)
    {
        string header = GetString(key);

        if (int.TryParse(header, out int value)) return value;

        throw new InvalidCastException($"The key '{key}' was not a valid int.");
    }
}
