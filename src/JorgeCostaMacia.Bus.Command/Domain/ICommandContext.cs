using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.Command.Domain;

/// <summary>Marker for a command handler context.</summary>
public interface ICommandContext : IMessageContext { }

/// <summary>
/// The context a command handler receives: the typed envelope — domain trace, messaging trace,
/// destination addresses, conversation and resilience — plus the transport-specific
/// <see cref="Metadata"/> escape hatch. <typeparamref name="TMetadata"/> is bound by the transport
/// (e.g. a Kafka metadata), so it never leaks onto the handler.
/// </summary>
/// <typeparam name="TMetadata">The transport's consume-metadata type.</typeparam>
/// <typeparam name="TCommand">The command type.</typeparam>
public interface ICommandContext<TMetadata, TCommand>
    : ICommandContext,
      IAggregateTracedMessageContext<TCommand>,
      IAggregateFilteredMessageContext<TCommand>,
      ITracedMessageContext<TCommand>,
      IConversationMessageContext<TCommand>,
      IResilientMessageContext<TCommand>
    where TMetadata : ITransportContext
    where TCommand : ICommand
{
    /// <summary>Transport-specific metadata for this delivery (headers, offset, …).</summary>
    TMetadata Metadata { get; }
}
