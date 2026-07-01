namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet surfacing the messaging-internal conversation trace: an id, origin address and
/// start time set by the first message of a conversation and propagated to every message that
/// follows, so the whole chain is traceable back to where and when it began. Assigned by the
/// messaging layer, not the domain — distinct from
/// <see cref="IAggregateTracedContext.AggregateCorrelationId"/>.
/// </summary>
public interface IConversationContext : IContext
{
    /// <summary>Conversation trace id, shared by the whole chain; equals the first message's id.</summary>
    Guid ConversationId { get; }

    /// <summary>Address (topic/queue) where the conversation originated — the first message's destination.</summary>
    string ConversationAddress { get; }

    /// <summary>UTC time when the conversation began — the first message's timestamp.</summary>
    DateTime ConversationOccurredAt { get; }
}
