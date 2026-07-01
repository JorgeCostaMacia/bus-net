using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Sends a message point-to-point correlated with an inbound context — the traced variant of
/// <see cref="ISenderBus{TMessage}"/>, for re-sending from inside a handler while propagating the
/// inbound message's correlation (and other envelope data).
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>ICommand</c>).</typeparam>
public interface ISenderTracedBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>
    /// Sends a message correlated with an inbound context, propagating its correlation (and other
    /// envelope data) — use this when re-sending from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="context">The inbound context whose correlation is propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, IAggregateTracedContext context, CancellationToken cancellationToken = default)
        where T : TMessage;
}
