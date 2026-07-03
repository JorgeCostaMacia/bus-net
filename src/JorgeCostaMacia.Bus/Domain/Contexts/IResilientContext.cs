namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the delivery-resilience counter for the inbound message, so a handler (or
/// a cross-cutting middleware) can react to redeliveries — e.g. log, branch, or give up on the last
/// attempt.
/// </summary>
public interface IResilientContext : IContext
{
    /// <summary>Number of times this message has been redelivered (immediate or scheduled).</summary>
    int RedeliveryCount { get; }
}
