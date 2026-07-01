using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>Marker for a command handler context.</summary>
public interface ICommandContext : IContext { }

/// <summary>
/// The context a command handler receives: the two real objects of the delivery — the
/// <typeparamref name="TCommand"/> and the <typeparamref name="TTransport"/> it arrived on — plus
/// the read-only envelope facets projected over them (domain trace, messaging trace, destination
/// addresses, conversation and resilience). <typeparamref name="TTransport"/> is bound by the
/// transport (e.g. a Kafka transport), so it never leaks onto the handler unless it asks for it.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
/// <typeparam name="TTransport">The transport the command arrived on.</typeparam>
public interface ICommandContext<TCommand, TTransport>
    : ICommandContext,
      IContext<TCommand, TTransport>,
      IAggregateTracedContext<TCommand>,
      IAggregateFilteredContext<TCommand>,
      ITracedContext<TCommand>,
      IConversationContext<TCommand>,
      IResilientContext<TCommand>
    where TCommand : ICommand
    where TTransport : ITransport
{ }
