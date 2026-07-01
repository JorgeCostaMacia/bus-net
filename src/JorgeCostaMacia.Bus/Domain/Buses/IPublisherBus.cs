namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Publishes a message to every interested subscriber (pub/sub). The transport maps the message
/// type to its topic/exchange, so you can publish a message instance directly.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>IEvent</c>).</typeparam>
public interface IPublisherBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Publishes a message, starting a new correlation flow.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : TMessage;
}
