using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>Builds the <c>jcm_</c> envelope headers carried alongside an outbound message.</summary>
internal static class HeadersFactory
{
    /// <summary>
    /// Fresh envelope for a new flow: a new message id, the conversation begins here (its id/address/
    /// time mirror this message), origin unknown, counters at zero. Domain trace comes from the message.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="topic">The destination topic.</param>
    /// <param name="message">The message being sent.</param>
    /// <returns>The Kafka headers for the delivery.</returns>
    public static Headers CreateNew<TMessage>(string topic, TMessage message)
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Guid messageId = JorgeCostaMacia.GuidFactory.Domain.GuidFactory.Create();
        string occurredAt = DateTime.UtcNow.ToString("O");
        Type type = message.GetType();

        return new Headers
        {
            { HeaderKeys.MessageId, Bytes(messageId) },
            { HeaderKeys.MessageType, Bytes(type.FullName ?? type.Name) },
            { HeaderKeys.MessageTypeUrn, JsonSerializer.SerializeToUtf8Bytes(JorgeCostaMacia.Bus.UrnFactory.Domain.UrnFactory.Create(type)) },
            { HeaderKeys.MessageDestinationAddress, Bytes(topic) },
            { HeaderKeys.MessageOccurredAt, Bytes(occurredAt) },
            { HeaderKeys.ConversationId, Bytes(messageId) },
            { HeaderKeys.ConversationAddress, Bytes(topic) },
            { HeaderKeys.ConversationOccurredAt, Bytes(occurredAt) },
            { HeaderKeys.AggregateId, Bytes(message.AggregateId) },
            { HeaderKeys.AggregateCorrelationId, Bytes(message.AggregateCorrelationId) },
            { HeaderKeys.AggregateOccurredAt, Bytes(message.AggregateOccurredAt.ToString("O")) },
            { HeaderKeys.AggregateDestinationAddresses, JsonSerializer.SerializeToUtf8Bytes(message.AggregateDestinationAddresses) },
            { HeaderKeys.RetryCount, Bytes("0") },
            { HeaderKeys.RedeliveryCount, Bytes("0") }
        };
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static byte[] Bytes(Guid value) => Encoding.UTF8.GetBytes(value.ToString());
}
