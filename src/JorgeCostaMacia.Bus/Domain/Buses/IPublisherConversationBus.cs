using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Publishes a message to every interested subscriber (pub/sub) continuing an inbound conversation —
/// the conversation-propagating variant of <see cref="IPublisherBus{TMessage}"/>, for publishing from
/// inside a handler. The message already carries its own domain trace (<c>ITracedMessage</c>), so only
/// the conversation is propagated here — plus, in the overload, the resilience counters, to control
/// redeliveries.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>IEvent</c>).</typeparam>
public interface IPublisherConversationBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Publishes a message continuing the given conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="conversation">The inbound conversation this message continues.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, IConversationContext conversation, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>Publishes a message continuing the conversation and carrying the resilience counters (for redelivery control).</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="conversation">The inbound conversation this message continues.</param>
    /// <param name="resilient">The inbound resilience counters (retry / redelivery) to propagate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(T message, IConversationContext conversation, IResilientContext resilient, CancellationToken cancellationToken = default)
        where T : TMessage;
}
