using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The topic mapping for an event type (type → topic routing). The topic itself is infrastructure —
/// auto-created by the broker and managed broker-side.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
public sealed record EventConfiguration<TEvent> : IMessageConfiguration
    where TEvent : IEvent
{
    /// <summary>The CLR type of the event.</summary>
    public Type MessageType { get; init; }

    /// <summary>The Kafka topic the event is published to.</summary>
    public string Topic { get; init; }

    /// <summary>Maps the event type to its topic.</summary>
    /// <param name="topic">The Kafka topic.</param>
    public EventConfiguration(string topic)
    {
        MessageType = typeof(TEvent);
        Topic = topic;
    }
}
