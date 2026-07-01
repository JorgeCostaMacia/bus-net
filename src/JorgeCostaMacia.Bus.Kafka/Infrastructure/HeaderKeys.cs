namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>The <c>jcm_</c>-prefixed Kafka header keys that carry the message envelope.</summary>
internal static class HeaderKeys
{
    public const string MessageId = "jcm_message_id";
    public const string MessageType = "jcm_message_type";
    public const string MessageTypeUrn = "jcm_message_type_urn";
    public const string MessageDestinationAddress = "jcm_message_destination_address";
    public const string MessageOriginAddress = "jcm_message_origin_address";
    public const string MessageOccurredAt = "jcm_message_occurred_at";
    public const string ConversationId = "jcm_conversation_id";
    public const string ConversationAddress = "jcm_conversation_address";
    public const string ConversationOccurredAt = "jcm_conversation_occurred_at";
    public const string AggregateId = "jcm_aggregate_id";
    public const string AggregateCorrelationId = "jcm_aggregate_correlation_id";
    public const string AggregateOccurredAt = "jcm_aggregate_occurred_at";
    public const string AggregateDestinationAddresses = "jcm_aggregate_destination_addresses";
    public const string RetryCount = "jcm_retry_count";
    public const string RedeliveryCount = "jcm_redelivery_count";
}
