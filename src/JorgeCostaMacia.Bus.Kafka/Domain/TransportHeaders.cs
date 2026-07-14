using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The header hub — the <c>jcm-</c>-prefixed Kafka header keys that carry the message envelope, which
/// keys travel as Guids/ints, how a value is encoded to header bytes (<see cref="ToHeader(string)"/>
/// and overloads), and how a key is re-stamped (<see cref="Restamp"/>). Reading the bytes back lives
/// on <see cref="Transport"/>, where the delivered headers are.
/// </summary>
internal static class TransportHeaders
{
    /// <summary>The common prefix shared by every envelope header key.</summary>
    public const string Prefix = "jcm-";

    public const string MessageId = Prefix + "message-id";
    public const string MessageType = Prefix + "message-type";
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
    public static readonly ImmutableList<string> GuidHeaders = ImmutableList.Create(
        MessageId,
        ConversationId,
        AggregateId,
        AggregateCorrelationId);

    /// <summary>The keys whose values travel as integer digits (the resilience counter).</summary>
    public static readonly ImmutableList<string> IntHeaders = ImmutableList.Create(RetryCount);

    /// <summary>Encodes a text value to header bytes (UTF-8).</summary>
    public static byte[] ToHeader(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Encodes a <see cref="Guid"/> to header bytes (16 raw bytes).</summary>
    public static byte[] ToHeader(Guid value) => value.ToByteArray();

    /// <summary>Encodes an integer to header bytes (its decimal digits).</summary>
    public static byte[] ToHeader(int value) => Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));

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

    /// <summary>
    /// Stamps a failure onto the (already cloned) envelope — the exception type and message, the
    /// failing group and the UTC time — so the parked delivery is filterable and reinjectable
    /// header-side. Shared by every error and fault handler so the stamp stays identical across lanes.
    /// </summary>
    /// <param name="headers">The headers to stamp — typically a clone of the delivery's envelope.</param>
    /// <param name="error">The failure whose type and message are stamped.</param>
    /// <param name="groupId">The failing consumer group id.</param>
    public static void StampError(Headers headers, Exception error, string groupId)
    {
        Type type = error.GetType();

        headers.Add(ErrorType, ToHeader(type.FullName ?? type.Name));
        headers.Add(ErrorMessage, ToHeader(error.Message));
        headers.Add(ErrorGroupId, ToHeader(groupId));
        headers.Add(ErrorOccurredAt, ToHeader(DateTime.UtcNow.ToString("O")));
    }
}
