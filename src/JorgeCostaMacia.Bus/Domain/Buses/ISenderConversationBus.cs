using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Sends a message point-to-point continuing an inbound conversation — the conversation-propagating
/// variant of <see cref="ISenderBus{TMessage}"/>, for re-sending from inside a handler. The message
/// already carries its own domain trace (<c>ITracedMessage</c>), so only the conversation is
/// propagated here — plus, in the overload, the resilience counters, to control redeliveries.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>ICommand</c>).</typeparam>
public interface ISenderConversationBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Sends a message continuing the given conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="conversation">The inbound conversation this message continues.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, IConversationContext conversation, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>Sends a message continuing the conversation and carrying the resilience counters (for redelivery control).</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="conversation">The inbound conversation this message continues.</param>
    /// <param name="resilient">The inbound resilience counters (retry / redelivery) to propagate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, IConversationContext conversation, IResilientContext resilient, CancellationToken cancellationToken = default)
        where T : TMessage;
}
