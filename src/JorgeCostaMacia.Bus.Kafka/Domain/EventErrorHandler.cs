using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Ergonomic base for an event's error handler — the app's own error management (log, compensate,
/// notify, decide), invoked when the event's subscriber fails terminally and the delivery parks to
/// <c>.error</c>: closes the <see cref="IHandler{TMessage, TContext}"/> contract over
/// <see cref="EventErrorContext{TEvent}"/>, so a concrete error handler declares only its event
/// type and gets fully-typed access to the failed event, its envelope and the exception.
/// </summary>
/// <typeparam name="TEvent">The event type whose terminal failures this handler manages.</typeparam>
internal abstract class EventErrorHandler<TEvent> : IHandler<TEvent, EventErrorContext<TEvent>>
    where TEvent : Event
{
    /// <summary>
    /// How the handler left the delivery — set as it runs, read by the consumer afterwards (since
    /// <see cref="Handle"/> returns <see cref="Task"/> by contract and cannot report it). Defaults to
    /// <see cref="ErrorHandlerResult.Unhandled"/> until the handler decides.
    /// </summary>
    public ErrorHandlerResult Result { get; protected set; }

    /// <summary>Handles the terminal failure of an event delivery.</summary>
    /// <param name="context">The delivery's error context — the failed event, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(EventErrorContext<TEvent> context, CancellationToken cancellationToken = default);
}
