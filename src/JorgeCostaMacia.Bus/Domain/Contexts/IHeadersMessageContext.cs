namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet for reading individual transport headers by key — with a typed key and value — so
/// a handler can read custom metadata without deserializing the message body. Whole-collection
/// access is intentionally omitted: propagation is driven from the traced context, not by forwarding
/// the raw header bag. Non-generic in the message type so it can be read without knowing it.
/// </summary>
/// <typeparam name="THeadersKey">The header key type (typically <see cref="string"/>).</typeparam>
/// <typeparam name="THeadersValue">The header value type (e.g. <c>byte[]</c> on Kafka).</typeparam>
public interface IHeadersMessageContext<THeadersKey, THeadersValue> : IMessageContext
{
    /// <summary>Reads a single header value by key.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value.</returns>
    THeadersValue GetHeadersValue(THeadersKey key);

    /// <summary>Tries to read a single header value by key.</summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">The header value, when present.</param>
    /// <returns><see langword="true"/> if the header exists; otherwise <see langword="false"/>.</returns>
    bool TryGetHeadersValue(THeadersKey key, out THeadersValue value);
}

/// <summary>The headers envelope facet bound to a specific inbound message type.</summary>
/// <typeparam name="THeadersKey">The header key type.</typeparam>
/// <typeparam name="THeadersValue">The header value type.</typeparam>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IHeadersMessageContext<THeadersKey, THeadersValue, TMessage>
    : IHeadersMessageContext<THeadersKey, THeadersValue>, IMessageContext<TMessage>
    where TMessage : IMessage
{ }
