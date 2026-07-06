namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Sends a batch of messages point-to-point in one call — the opt-in batch counterpart of
/// <see cref="ISenderBus{TMessage}"/>. The messages are enqueued in order and awaited together, which
/// is the efficient way to dispatch many at once (e.g. an orchestrator that computes N units of work
/// and fans out one command per unit).
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. the transport's <c>Command</c> base).</typeparam>
public interface ISenderBatchBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Sends a batch of messages, each starting a new conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="messages">The messages to send.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>
    /// Sends a batch of messages continuing from an inbound delivery: the inbound
    /// <paramref name="transport"/>'s envelope (conversation, resilience counters) is propagated to
    /// each. Use this when fanning out from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="messages">The messages to send.</param>
    /// <param name="transport">The inbound message's transport, whose envelope headers are propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Send<T>(IEnumerable<T> messages, ITransport transport, CancellationToken cancellationToken = default)
        where T : TMessage;
}
