using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;

/// <summary>Header helpers shared by everything that re-stamps an outbound envelope.</summary>
internal static class HeadersExtensions
{
    /// <summary>Replaces every value of a header key with the given one.</summary>
    /// <param name="headers">The headers to restamp.</param>
    /// <param name="key">The header key.</param>
    /// <param name="value">The new value.</param>
    public static void Restamp(this Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}
