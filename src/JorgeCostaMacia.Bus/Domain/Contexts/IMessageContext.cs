namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the delivered message itself — one of the two real objects a delivery
/// carries (alongside the transport via <see cref="ITransportContext{TTransport}"/>). Every other
/// facet is a read-only projection over the message's envelope; this one is the message. Typically
/// the typed <see cref="IMessage"/>; fault contexts close it over a raw representation (e.g.
/// <see cref="string"/>) when the message could not be deserialized.
/// </summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IMessageContext<TMessage> : IContext
{
    /// <summary>The delivered message.</summary>
    TMessage Message { get; }
}
