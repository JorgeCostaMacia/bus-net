namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Publishes a message to every interested subscriber (pub/sub). The transport maps the message type
/// to its topic/exchange, so you can publish a message instance directly. Publish it plain to start a
/// new conversation, or pass the inbound transport to continue from a message being handled — its
/// envelope (conversation, resilience counters) is propagated from there.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>IEvent</c>).</typeparam>
public interface IPublisherBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Publishes a message, starting a new conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>
    /// Publishes a message continuing from an inbound delivery: the inbound <paramref name="transport"/>
    /// carries the envelope (conversation, resilience counters) to propagate. Use this when publishing
    /// from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="transport">The inbound message's transport, whose envelope headers are propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : TMessage;
}
