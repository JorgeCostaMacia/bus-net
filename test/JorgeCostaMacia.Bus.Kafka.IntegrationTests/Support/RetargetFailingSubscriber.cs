using JorgeCostaMacia.Bus.Kafka.Domain.Events;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;

/// <summary>
/// The subscriber whose group fails once: it throws on the first delivery so the bus's event error
/// handler re-produces the event immediately, re-targeted to this group only via the
/// <c>AggregateConsumers</c> header, then succeeds on that redelivery — recording both invocations on
/// the shared <see cref="RetargetProbe"/> the test awaits.
/// </summary>
public sealed class RetargetFailingSubscriber : EventSubscriber<RequeueEvent>
{
    private readonly RetargetProbe _probe;

    /// <summary>Takes the shared probe the subscriber records onto and the test awaits.</summary>
    /// <param name="probe">The invocation signal shared with the test.</param>
    public RetargetFailingSubscriber(RetargetProbe probe)
        => _probe = probe;

    /// <summary>Fails the first delivery to force an immediate, group-targeted requeue; records and succeeds on the redelivery.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(EventContext<RequeueEvent> context, CancellationToken cancellationToken = default)
    {
        int attempt = _probe.RecordFailing();

        if (attempt == 1) throw new InvalidOperationException("Failing the first delivery on purpose so the retry is re-produced immediately, re-targeted to this group only.");

        return Task.CompletedTask;
    }
}
