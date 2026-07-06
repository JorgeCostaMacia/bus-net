namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Sends a message point-to-point (one consumer). The transport maps the message type to its
/// route/queue, so you can send a message instance directly. Send it plain to start a new
/// conversation, or pass the inbound transport to continue from a message being handled — its
/// envelope (conversation, resilience counters) is propagated from there.
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. the transport's <c>Command</c> base).</typeparam>
public interface ISenderBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Sends a message, starting a new conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>
    /// Sends a message continuing from an inbound delivery: the inbound <paramref name="transport"/>
    /// carries the envelope (conversation, resilience counters) to propagate. Use this when re-sending
    /// from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="message">The message to send.</param>
    /// <param name="transport">The inbound message's transport, whose envelope headers are propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(T message, ITransport transport, CancellationToken cancellationToken = default)
        where T : TMessage;
}
