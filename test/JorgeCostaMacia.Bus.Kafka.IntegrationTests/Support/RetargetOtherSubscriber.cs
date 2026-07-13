using JorgeCostaMacia.Bus.Kafka.Domain.Events;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;

/// <summary>
/// The other subscriber group in the retry re-targeting test: it always succeeds and simply records
/// each invocation on the shared <see cref="RetargetProbe"/>. It receives the original event (empty
/// <c>AggregateConsumers</c> — not filtered) but must NOT receive the failing group's retry, which is
/// re-targeted to that group only — so the test asserts this group ran exactly once.
/// </summary>
public sealed class RetargetOtherSubscriber : EventSubscriber<RequeueEvent>
{
    private readonly RetargetProbe _probe;

    /// <summary>Takes the shared probe the subscriber records onto and the test awaits.</summary>
    /// <param name="probe">The invocation signal shared with the test.</param>
    public RetargetOtherSubscriber(RetargetProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Records its one invocation — the original delivery, never the failing group's re-targeted retry.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(EventContext<RequeueEvent> context, CancellationToken cancellationToken = default)
    {
        _probe.RecordOther();

        return Task.CompletedTask;
    }
}
