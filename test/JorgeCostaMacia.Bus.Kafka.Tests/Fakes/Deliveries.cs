using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// Delivery, transport and header builders shared across the consumer tests — one place so the error,
/// fault and worker tests all drive the loop off the same envelope shape.
/// </summary>
internal static class Deliveries
{
    /// <summary>The topic the tests produce and consume on.</summary>
    public const string TOPIC = "orders";

    /// <summary>The consumer group id the tests run under.</summary>
    public const string GROUP_ID = "orders.handler";

    /// <summary>A transport over a minimal envelope (retry count + aggregate trace) — for the error/fault handler tests that only need the transport.</summary>
    public static Transport Transport(int retryCount = 0, Guid? aggregateId = null, Guid? aggregateCorrelationId = null)
        => Domain.Transport.Create(Result("{}"u8.ToArray(), TraceHeaders(retryCount, aggregateId, aggregateCorrelationId)));

    /// <summary>A well-formed delivery carrying the serialized message and the aggregate trace; <paramref name="consumers"/> stamps the <c>AggregateConsumers</c> header for the event filtering tests.</summary>
    public static ConsumeResult<Ignore, byte[]> Delivery<TMessage>(TMessage message, long offset = 10, string? consumers = null)
    {
        Headers headers = TraceHeaders();

        if (consumers is not null) headers.Add(TransportHeaders.AggregateConsumers, Encoding.UTF8.GetBytes(consumers));

        return Result(JsonSerializer.SerializeToUtf8Bytes(message), headers, offset);
    }

    /// <summary>A delivery whose body is not valid JSON — drives the malformed/fault path.</summary>
    public static ConsumeResult<Ignore, byte[]> Garbage(long offset = 10) => Result("}{ not json"u8.ToArray(), [], offset);

    /// <summary>A delivery whose body deserializes to <see langword="null"/> — drives the null-body/fault path.</summary>
    public static ConsumeResult<Ignore, byte[]> NullBody(long offset = 10) => Result("null"u8.ToArray(), [], offset);

    /// <summary>Reads a header as UTF-8 text, or <see langword="null"/> when absent.</summary>
    public static string? Header(Message<Null, byte[]> message, string key)
        => message.Headers.TryGetLastBytes(key, out byte[] value) ? Encoding.UTF8.GetString(value) : null;

    private static Headers TraceHeaders(int retryCount = 0, Guid? aggregateId = null, Guid? aggregateCorrelationId = null)
        =>
        [
            new Header(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retryCount.ToString())),
            new Header(TransportHeaders.AggregateId, (aggregateId ?? Guid.NewGuid()).ToByteArray()),
            new Header(TransportHeaders.AggregateCorrelationId, (aggregateCorrelationId ?? Guid.NewGuid()).ToByteArray())
        ];

    private static ConsumeResult<Ignore, byte[]> Result(byte[] value, Headers headers, long offset = 10)
        => new()
        {
            TopicPartitionOffset = new TopicPartitionOffset(TOPIC, new Partition(0), new Offset(offset)),
            Message = new Message<Ignore, byte[]> { Value = value, Headers = headers }
        };
}
