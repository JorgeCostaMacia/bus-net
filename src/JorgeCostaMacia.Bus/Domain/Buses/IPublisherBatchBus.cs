namespace JorgeCostaMacia.Bus.Domain.Buses;

/// <summary>
/// Publishes a batch of messages (pub/sub) in one call — the opt-in batch counterpart of
/// <see cref="IPublisherBus{TMessage}"/>. The messages are enqueued in order and awaited together, the
/// efficient way to publish many at once (e.g. all the events an aggregate pulled in a single unit of
/// work).
/// </summary>
/// <typeparam name="TMessage">The message family this bus accepts (e.g. the transport's <c>Event</c> base).</typeparam>
public interface IPublisherBatchBus<TMessage> : IBus
    where TMessage : IMessage
{
    /// <summary>Publishes a batch of messages, each starting a new conversation.</summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="messages">The messages to publish.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : TMessage;

    /// <summary>
    /// Publishes a batch of messages continuing from an inbound delivery: the inbound
    /// <paramref name="transport"/>'s envelope (conversation, resilience counters) is propagated to
    /// each. Use this when fanning out from inside a handler.
    /// </summary>
    /// <typeparam name="T">The concrete message type.</typeparam>
    /// <param name="messages">The messages to publish.</param>
    /// <param name="transport">The inbound message's transport, whose envelope headers are propagated.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Publish<T>(IEnumerable<T> messages, ITransport transport, CancellationToken cancellationToken = default)
        where T : TMessage;
}
