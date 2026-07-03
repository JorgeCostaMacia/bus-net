namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for a message handler, so handlers can be recognised and registered under a single
/// abstraction. The handling contract itself lives on the typed variant.
/// </summary>
public interface IHandler { }

/// <summary>
/// Handles a delivered <typeparamref name="TMessage"/>, receiving a context of the exact shape
/// (<typeparamref name="TContext"/>) it needs — compose the context from the facets in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>. Completing the returned task acknowledges the
/// message; throwing triggers the transport's retry policy.
/// </summary>
/// <typeparam name="TMessage">The message type this handler processes.</typeparam>
/// <typeparam name="TContext">The context shape the handler requires for that message.</typeparam>
public interface IHandler<TMessage, TContext> : IHandler
    where TMessage : IMessage
    where TContext : IContext
{
    /// <summary>Handles the message carried by <paramref name="context"/>.</summary>
    /// <param name="context">The inbound message context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Handle(TContext context, CancellationToken cancellationToken = default);
}
