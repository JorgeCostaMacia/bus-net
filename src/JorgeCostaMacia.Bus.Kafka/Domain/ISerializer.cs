namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Serializes a message body to/from the bytes carried as the Kafka value. The default is JSON
/// (System.Text.Json); swap the implementation to change the wire format (e.g. a binary one).
/// </summary>
public interface ISerializer
{
    /// <summary>Serializes a message to its byte payload.</summary>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The serialized bytes.</returns>
    byte[] Serialize(object message);

    /// <summary>Deserializes a byte payload back into the given type.</summary>
    /// <param name="data">The serialized bytes.</param>
    /// <param name="type">The concrete message type to deserialize into (resolved from the headers).</param>
    /// <returns>The deserialized message, or <see langword="null"/>.</returns>
    object? Deserialize(byte[] data, Type type);
}
