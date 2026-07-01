namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing delivery-resilience counters for the inbound message, so a handler (or a
/// cross-cutting middleware) can react to retries/redeliveries — e.g. log, branch, or give up on the
/// last attempt. Non-generic so it can be read without knowing the message type.
/// </summary>
public interface IResilientMessageContext : IMessageContext
{
    /// <summary>Number of in-process retry attempts made for this delivery.</summary>
    int RetryCount { get; }

    /// <summary>Number of times this message has been redelivered by the transport.</summary>
    int RedeliveryCount { get; }
}

/// <summary>The resilience envelope facet bound to a specific inbound message type.</summary>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IResilientMessageContext<TMessage> : IResilientMessageContext, IMessageContext<TMessage>
    where TMessage : IMessage
{ }
