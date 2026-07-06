using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The header hub — the <c>jcm_</c>-prefixed Kafka header keys that carry the message envelope, which
/// keys travel as Guids/ints, how a value is encoded to header bytes (<see cref="ToHeader(string)"/>
/// and overloads), and how a key is re-stamped (<see cref="Restamp"/>). Reading the bytes back lives
/// on <see cref="Transport"/>, where the delivered headers are.
/// </summary>
internal static class TransportHeaders
{
    /// <summary>The common prefix shared by every envelope header key.</summary>
    public const string Prefix = "jcm_";

    public const string MessageId = Prefix + "message_id";
    public const string MessageType = Prefix + "message_type";
    public const string MessageTypeUrn = Prefix + "message_type_urn";
    public const string MessageDestinationAddress = Prefix + "message_destination_address";
    public const string MessageOriginAddress = Prefix + "message_origin_address";
    public const string MessageOccurredAt = Prefix + "message_occurred_at";
    public const string ConversationId = Prefix + "conversation_id";
    public const string ConversationAddress = Prefix + "conversation_address";
    public const string ConversationOccurredAt = Prefix + "conversation_occurred_at";
    public const string AggregateId = Prefix + "aggregate_id";
    public const string AggregateCorrelationId = Prefix + "aggregate_correlation_id";
    public const string AggregateOccurredAt = Prefix + "aggregate_occurred_at";
    public const string AggregateConsumers = Prefix + "aggregate_consumers";
    public const string RetryCount = Prefix + "retry_count";
    public const string ErrorType = Prefix + "error_type";
    public const string ErrorMessage = Prefix + "error_message";
    public const string ErrorGroupId = Prefix + "error_group_id";
    public const string ErrorOccurredAt = Prefix + "error_occurred_at";
    public const string HostMachineName = Prefix + "host_machine_name";
    public const string HostAssembly = Prefix + "host_assembly";
    public const string HostAssemblyVersion = Prefix + "host_assembly_version";
    public const string HostFrameworkVersion = Prefix + "host_framework_version";
    public const string HostBusVersion = Prefix + "host_bus_version";
    public const string HostOperatingSystemVersion = Prefix + "host_operating_system_version";

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
    public static byte[] ToHeader(int value) => Encoding.UTF8.GetBytes(value.ToString());

    /// <summary>Encodes a string list to header bytes (comma-joined, UTF-8).</summary>
    public static byte[] ToHeader(IEnumerable<string> values) => Encoding.UTF8.GetBytes(string.Join(',', values));

    /// <summary>Replaces every value of a header key with the given one — a plain static (not an extension) so it can never collide with a future Kafka client method.</summary>
    /// <param name="headers">The headers to restamp.</param>
    /// <param name="key">The header key.</param>
    /// <param name="value">The new value.</param>
    public static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}
