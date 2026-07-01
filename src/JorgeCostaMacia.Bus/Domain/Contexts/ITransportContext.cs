namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the transport the message arrived on — one of the two real objects a
/// delivery carries (alongside the message via <see cref="IMessageContext{TMessage}"/>). Concrete
/// transports expose whatever they actually provide (e.g. a Kafka transport with topic / partition /
/// offset and typed header getters); transport-agnostic reads go through the other facets, not this.
/// </summary>
/// <typeparam name="TTransport">The transport the message arrived on.</typeparam>
public interface ITransportContext<TTransport> : IContext
    where TTransport : ITransport
{
    /// <summary>The transport this message arrived on.</summary>
    TTransport Transport { get; }
}
