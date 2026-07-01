namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the delivered message itself — one of the two real objects a delivery
/// carries (alongside the transport via <see cref="ITransportContext{TTransport}"/>). Every other
/// facet is a read-only projection over the message's envelope; this one is the message.
/// </summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IMessageContext<TMessage> : IContext
    where TMessage : IMessage
{
    /// <summary>The delivered message.</summary>
    TMessage Message { get; }
}
