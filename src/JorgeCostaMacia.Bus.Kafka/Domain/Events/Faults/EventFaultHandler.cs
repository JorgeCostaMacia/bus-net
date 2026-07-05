using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;

/// <summary>
/// An event's fault handler — manages a broken event delivery (undeserializable body, unreadable
/// envelope, or a failure the event error handler could not cope with): parks an
/// <see cref="EventFault"/> to <c>.fault</c> over the raw body, transport and failure. Closes the
/// <see cref="IHandler{TMessage, TContext}"/> contract over <see cref="EventFaultContext"/> on its
/// own — nothing shared with the command side, so the event side stays a fully independent, distinct
/// type. The broken delivery carries no typed message, so the parked body is raw; the type parameters
/// only tie it to its pairing — they do not shape the parked message.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
/// <typeparam name="TEventSubscriber">The event subscriber this fault handler is paired with.</typeparam>
internal abstract class EventFaultHandler<TEvent, TEventSubscriber> : IHandler<IMessage, EventFaultContext>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    /// <summary>
    /// How the handler left the delivery — set as it runs, read by the consumer afterwards (since
    /// <see cref="Handle"/> returns <see cref="Task"/> by contract and cannot report it). Defaults to
    /// <see cref="FaultResult.Unhandled"/> until the handler decides.
    /// </summary>
    public FaultResult Result { get; protected set; }

    /// <summary>Handles a broken event delivery.</summary>
    /// <param name="context">The fault context — the raw body as text, the transport and the failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(EventFaultContext context, CancellationToken cancellationToken = default);
}
