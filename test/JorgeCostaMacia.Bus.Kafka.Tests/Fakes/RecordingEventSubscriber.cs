using JorgeCostaMacia.Bus.Kafka.Domain.Events;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>Event subscriber double — records the event it received and optionally fails, to drive the consume loop's success, error and filtering paths.</summary>
internal sealed class RecordingEventSubscriber : EventSubscriber<TestEvent>
{
    /// <summary>The event the loop handed to the subscriber, or <see langword="null"/> if it never ran.</summary>
    public TestEvent? Received { get; private set; }

    /// <summary>An exception to fail with, or <see langword="null"/> to succeed.</summary>
    public Exception? Failure { get; set; }

    public override Task Handle(EventContext<TestEvent> context, CancellationToken cancellationToken = default)
    {
        Received = context.Message;

        return Failure is not null ? Task.FromException(Failure) : Task.CompletedTask;
    }
}
