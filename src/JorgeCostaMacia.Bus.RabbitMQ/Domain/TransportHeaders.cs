using System.Collections.Immutable;
using System.Globalization;
using System.Text;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The header hub — the <c>jcm-</c>-prefixed header keys that carry the message envelope, which keys
/// travel as Guids/ints, how a value is encoded to header bytes (<see cref="ToHeader(string)"/> and
/// overloads), and how a key is re-stamped (<see cref="Restamp"/>). Values travel as raw bytes in the
/// AMQP field table (RabbitMQ returns string headers as bytes anyway), matching the Kafka encoding.
/// Reading the bytes back lives on <see cref="Transport"/>.
/// </summary>
internal static class TransportHeaders
{
    /// <summary>The common prefix shared by every envelope header key.</summary>
    public const string Prefix = "jcm-";

    public const string MessageId = Prefix + "message-id";
    public const string MessageType = Prefix + "message-type";
    public const string MessageTypeUrn = Prefix + "message-type-urn";
    public const string MessageDestinationAddress = Prefix + "message-destination-address";
    public const string MessageOriginAddress = Prefix + "message-origin-address";
    public const string MessageOccurredAt = Prefix + "message-occurred-at";
    public const string ConversationId = Prefix + "conversation-id";
    public const string ConversationAddress = Prefix + "conversation-address";
    public const string ConversationOccurredAt = Prefix + "conversation-occurred-at";
    public const string AggregateId = Prefix + "aggregate-id";
    public const string AggregateCorrelationId = Prefix + "aggregate-correlation-id";
    public const string AggregateOccurredAt = Prefix + "aggregate-occurred-at";
    public const string AggregateConsumers = Prefix + "aggregate-consumers";
    public const string RetryCount = Prefix + "retry-count";
    public const string ErrorType = Prefix + "error-type";
    public const string ErrorMessage = Prefix + "error-message";
    public const string ErrorGroupId = Prefix + "error-group-id";
    public const string ErrorOccurredAt = Prefix + "error-occurred-at";
    public const string HostMachineName = Prefix + "host-machine-name";
    public const string HostAssembly = Prefix + "host-assembly";
    public const string HostAssemblyVersion = Prefix + "host-assembly-version";
    public const string HostFrameworkVersion = Prefix + "host-framework-version";
    public const string HostBusVersion = Prefix + "host-bus-version";
    public const string HostOperatingSystemVersion = Prefix + "host-operating-system-version";

    /// <summary>The keys whose values travel as 16 raw <see cref="Guid"/> bytes.</summary>
    public static readonly ImmutableList<string> GuidHeaders =
    [
        MessageId,
        ConversationId,
        AggregateId,
        AggregateCorrelationId
    ];

    /// <summary>The keys whose values travel as integer digits (the resilience counter).</summary>
    public static readonly ImmutableList<string> IntHeaders =
    [
        RetryCount
    ];

    /// <summary>Encodes a text value to header bytes (UTF-8).</summary>
    public static byte[] ToHeader(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Encodes a <see cref="Guid"/> to header bytes (16 raw bytes).</summary>
    public static byte[] ToHeader(Guid value) => value.ToByteArray();

    /// <summary>Encodes an integer to header bytes (its decimal digits).</summary>
    public static byte[] ToHeader(int value) => Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));

    /// <summary>Encodes a string list to header bytes (comma-joined, UTF-8).</summary>
    public static byte[] ToHeader(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));

    /// <summary>Sets a header key to the given value on an outbound header table (replacing any existing entry).</summary>
    /// <param name="headers">The outbound header table.</param>
    /// <param name="key">The header key.</param>
    /// <param name="value">The value bytes.</param>
    public static void Restamp(IDictionary<string, object?> headers, string key, byte[] value) => headers[key] = value;
}
