using System.Text.Json;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>The default <see cref="ISerializer"/> — UTF-8 JSON via System.Text.Json.</summary>
internal sealed class Serializer : ISerializer
{
    public byte[] Serialize(object message) => JsonSerializer.SerializeToUtf8Bytes(message, message.GetType());

    public object? Deserialize(byte[] data, Type type) => JsonSerializer.Deserialize(data, type);
}
