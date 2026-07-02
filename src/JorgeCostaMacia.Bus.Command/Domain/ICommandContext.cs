using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>Marker for a command handler context.</summary>
public interface ICommandContext : IContext { }

/// <summary>
/// The context a command handler receives — the glue that binds the whole delivery together: the
/// <typeparamref name="TCommand"/> and the <typeparamref name="TTransport"/> it arrived on, plus the
/// read-only envelope facets (messaging trace, domain trace, target consumers, conversation and
/// resilience). <typeparamref name="TTransport"/> is bound by the transport (e.g. a Kafka transport),
/// so it never leaks onto the handler unless it asks for it.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
/// <typeparam name="TTransport">The transport the command arrived on.</typeparam>
public interface ICommandContext<TCommand, TTransport>
    : ICommandContext,
      IMessageContext<TCommand>,
      ITransportContext<TTransport>,
      ITracedContext,
      IAggregateTracedContext,
      IAggregateFilteredContext,
      IConversationContext,
      IResilientContext
    where TCommand : ICommand
    where TTransport : ITransport
{ }
