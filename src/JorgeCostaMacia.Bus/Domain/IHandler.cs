namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for a handler, so every handler kind can be recognised and registered under a single
/// abstraction.
/// </summary>
public interface IHandler { }

/// <summary>
/// The single handling contract: handles a delivery of <typeparamref name="TMessage"/>, receiving a
/// context of the exact shape (<typeparamref name="TContext"/>) it needs — compose the context from
/// the facets in <c>JorgeCostaMacia.Bus.Domain.Contexts</c>. Message handlers, error handlers and
/// fault handlers all close this one contract over their own context; for a message delivery,
/// completing the returned task acknowledges the message and throwing triggers the transport's
/// retry policy.
/// </summary>
/// <typeparam name="TMessage">The message type this handler processes.</typeparam>
/// <typeparam name="TContext">The context shape the handler requires for that message.</typeparam>
public interface IHandler<TMessage, TContext> : IHandler
    where TMessage : IMessage
    where TContext : IContext
{
    /// <summary>Handles the delivery carried by <paramref name="context"/>.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Handle(TContext context, CancellationToken cancellationToken = default);
}
