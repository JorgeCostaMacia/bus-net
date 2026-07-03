using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// Ergonomic base for a Kafka event subscriber: closes the <see cref="IHandler{TMessage, TContext}"/>
/// contract over <see cref="EventContext{TEvent}"/>, so a concrete subscriber declares only its
/// event type and gets fully-typed access to the message, transport and envelope.
/// </summary>
/// <typeparam name="TEvent">The event type this subscriber processes.</typeparam>
public abstract class EventSubscriber<TEvent> : IHandler<TEvent, EventContext<TEvent>>
    where TEvent : Event
{
    /// <summary>Handles the delivered event.</summary>
    /// <param name="context">The delivery's context — event, transport and read-only envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(EventContext<TEvent> context, CancellationToken cancellationToken = default);
}
