namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for the context a handler receives when a message is delivered — the read-only envelope
/// around an inbound message. Composable via the facet interfaces in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>.
/// </summary>
public interface IContext { }

/// <summary>
/// The full delivery view: the two real objects that exist on every delivery — the
/// <typeparamref name="TMessage"/> and the <typeparamref name="TTransport"/> it arrived on. The
/// envelope facets are read-only projections over these; a concrete context composes the facets it
/// surfaces on top of this pair. <typeparamref name="TTransport"/> is bound by the transport
/// (e.g. a Kafka transport with topic / partition / offset and typed header getters), so it never
/// leaks onto the handler unless the handler asks for it.
/// </summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
/// <typeparam name="TTransport">The transport this message arrived on.</typeparam>
public interface IContext<out TMessage, out TTransport> : IContext
    where TMessage : IMessage
    where TTransport : ITransport
{
    /// <summary>The delivered message.</summary>
    TMessage Message { get; }

    /// <summary>The transport this message arrived on (headers, offset, transport-specific access).</summary>
    TTransport MessageTransport { get; }
}
