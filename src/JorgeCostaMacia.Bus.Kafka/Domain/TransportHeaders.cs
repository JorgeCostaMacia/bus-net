namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>The <c>jcm_</c>-prefixed Kafka header keys that carry the message envelope on the transport.</summary>
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
    public const string AggregateDestinationAddresses = Prefix + "aggregate_destination_addresses";
    public const string RetryCount = Prefix + "retry_count";
    public const string RedeliveryCount = Prefix + "redelivery_count";
}
