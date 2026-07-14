namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The expansions stamped as the <c>BusDescription</c> scope property on every outcome log: the
/// template is the minimal, groupable fact (small cardinality), and the description expands it with
/// the explanation or consequence — what the bus did about it or what happens next.
/// </summary>
internal static class BusLoggerDescriptions
{
    public const string RequeuedToRetry = "Requeued to retry.";
    public const string ScheduledToRetry = "Scheduled to retry.";
    public const string ParkedToErrorTopic = "Parked to the error topic.";
    public const string ParkedToFaultTopic = "Parked to the fault topic.";
    public const string DeliveryNotAcked = "The delivery is not acked.";
    public const string DeliveryBuried = "The delivery is not parked and not acked; the next commit on its partition buries it — restart before that for a redelivery, or re-inject from the topic.";
    public const string ScheduleFailed = "The scheduling failed; the delivery is not acked.";
    public const string RetrySchedulerMissing = "No retry scheduler is registered; parked to the error topic as terminal.";
    public const string HandedToFaultHandler = "The envelope is unreadable; handed to the fault handler.";
    public const string EscalatedToFaultHandler = "The error handler failed; escalated to the fault handler.";
    public const string SendFaulted = "The send faulted.";
    public const string ProducerQueueFull = "The producer's local queue is full; back-pressure upstream or raise QueueBufferingMaxMessages.";
    public const string ConsumeRetried = "The consume is retried.";
    public const string ConsumeLoopFailed = "An unexpected failure in the consume loop; backing off before the next consume.";
    public const string WorkerStopped = "The worker stopped.";
    public const string WorkerAbandoned = "The stop grace period expired; the consumer leaves the group by session timeout and is reclaimed at process exit.";
    public const string RedeliveredToNewOwner = "Lost in a rebalance; the new owner will handle the message again.";
    public const string RedeliveryWindowWidened = "The stored offsets were not committed; the crash-redelivery window widened.";
    public const string ApplicationStopped = "The client is unrecoverable; the application stops to restart clean.";
    public const string QueuedMessagesMayBeLost = "Queued messages may be lost.";
}
