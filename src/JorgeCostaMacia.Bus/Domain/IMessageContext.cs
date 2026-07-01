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
