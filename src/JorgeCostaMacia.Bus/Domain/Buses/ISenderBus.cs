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
}
