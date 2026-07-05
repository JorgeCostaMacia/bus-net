using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Faults;

/// <summary>
/// Ergonomic base for a fault handler — manages a broken delivery (undeserializable body, unreadable
/// envelope, or a failure the error handler could not cope with): a broken delivery carries no typed
/// message (the <see cref="FaultContext"/> is raw text, transport and failure), so nothing
/// differentiates faults per message and one handler self-contains the whole management. Closes the
/// <see cref="IHandler{TMessage, TContext}"/> contract over <see cref="FaultContext"/>.
/// </summary>
internal abstract class FaultHandler : IHandler<IMessage, FaultContext>
{
    /// <summary>
    /// How the handler left the delivery — set as it runs, read by the consumer afterwards (since
    /// <see cref="Handle"/> returns <see cref="Task"/> by contract and cannot report it). Defaults to
    /// <see cref="FaultHandlerResult.Unhandled"/> until the handler decides.
    /// </summary>
    public FaultHandlerResult Result { get; protected set; }

    /// <summary>Handles a broken delivery.</summary>
    /// <param name="context">The fault context — the raw body as text, the transport and the failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(FaultContext context, CancellationToken cancellationToken = default);
}
