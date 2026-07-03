namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the failure that broke a delivery — the live error at failure time,
/// typed by the context that composes it. Covariant, so a context closed over a specific error
/// type flows wherever the general <see cref="Exception"/> shape is required. In-process only: it
/// exists while the transport invokes the message's error management and is never serialized (the
/// transports' parking carries its own body).
/// </summary>
/// <typeparam name="TError">The type of the failure the context carries.</typeparam>
public interface IErrorContext<out TError> : IContext
    where TError : Exception
{
    /// <summary>The failure that broke the delivery.</summary>
    TError Error { get; }
}
