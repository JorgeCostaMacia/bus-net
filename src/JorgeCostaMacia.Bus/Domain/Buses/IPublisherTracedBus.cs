using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Publishes a message to every interested subscriber (pub/sub) correlated with an inbound context —
/// the traced variant of <see cref="IPublisherBus{TMessage}"/>, for publishing from inside a handler
/// while propagating the inbound message's correlation (and other envelope data).
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>IEvent</c>).</typeparam>
public interface IPublisherTracedBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>
    /// Publishes a message correlated with an inbound context, propagating its correlation (and
    /// other envelope data) — use this when publishing from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="context">The inbound context whose correlation is propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, IAggregateTracedContext context, CancellationToken cancellationToken = default)
        where T : TMessage;
}
