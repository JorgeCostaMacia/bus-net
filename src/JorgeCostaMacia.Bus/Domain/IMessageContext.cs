namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for the context a handler receives when a message is delivered — the read-only
/// envelope around an inbound message. Composable via the facet interfaces in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>.
/// </summary>
public interface IMessageContext { }

/// <summary>
/// The context around an inbound <typeparamref name="TMessage"/>: pure data, exposing the message
/// itself plus whatever envelope facets the concrete context implements.
/// </summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IMessageContext<TMessage> : IMessageContext
    where TMessage : IMessage
{
    /// <summary>The delivered message.</summary>
    TMessage Message { get; }
}

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
public interface IMessageContext<TMessage, TTransport> : IMessageContext<TMessage>
    where TMessage : IMessage
    where TTransport : ITransport
{
    /// <summary>The transport this message arrived on (headers, offset, transport-specific access).</summary>
    TTransport Transport { get; }
}
