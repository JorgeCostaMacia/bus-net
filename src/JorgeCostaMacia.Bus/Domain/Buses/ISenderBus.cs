using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Sends a message point-to-point (one consumer). The transport maps the message type to its
/// route/queue, so you can send a message instance directly without naming a destination.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. <c>ICommand</c>).</typeparam>
public interface ISenderBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Sends a message, starting a new correlation flow.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>
    /// Sends a message correlated with an inbound context, propagating its correlation (and other
    /// envelope data) — use this when re-sending from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="correlateWith">The inbound context whose correlation is propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, IAggregateTracedMessageContext correlateWith, CancellationToken cancellationToken = default)
        where T : TMessage;
}
