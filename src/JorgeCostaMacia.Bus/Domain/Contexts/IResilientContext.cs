namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing delivery-resilience counters for the inbound message, so a handler (or a
/// cross-cutting middleware) can react to retries/redeliveries — e.g. log, branch, or give up on the
/// last attempt.
/// </summary>
public interface IResilientContext : IContext
{
    /// <summary>Number of in-process retry attempts made for this delivery.</summary>
    int RetryCount { get; }

    /// <summary>Number of times this message has been redelivered by the transport.</summary>
    int RedeliveryCount { get; }
}
