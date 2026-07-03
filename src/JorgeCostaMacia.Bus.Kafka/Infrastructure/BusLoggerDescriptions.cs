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
    public const string DeliveryNotAcked = "The delivery is not acked.";
    public const string ScheduleFailed = "The scheduling failed; the delivery is not acked.";
    public const string RetrySchedulerMissing = "No retry scheduler is registered; the delivery is not acked.";
    public const string SendFaulted = "The send faulted.";
    public const string ConsumeRetried = "The consume is retried.";
    public const string WorkerStopped = "The worker stopped.";
    public const string WorkerAbandoned = "The stop grace period expired; the consumer leaves the group by session timeout and is reclaimed at process exit.";
    public const string RedeliveredToNewOwner = "Lost in a rebalance; the new owner will handle the message again.";
    public const string QueuedMessagesMayBeLost = "Queued messages may be lost.";
}
