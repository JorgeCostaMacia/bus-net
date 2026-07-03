namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consequence vocabulary stamped as the <c>Action</c> scope property on every outcome log —
/// the template states the fact, the <c>Action</c> states what the bus did about it or what happens
/// next. A low-cardinality label, so the bus's failures are indexable and manageable from the log
/// platform (panels and alerts per consequence, no template matching).
/// </summary>
internal static class BusLoggerActions
{
    public const string RequeuedToRetry = nameof(RequeuedToRetry);
    public const string ScheduledToRetry = nameof(ScheduledToRetry);
    public const string ParkedToErrorTopic = nameof(ParkedToErrorTopic);
    public const string DeliveryNotAcked = nameof(DeliveryNotAcked);
    public const string SendFaulted = nameof(SendFaulted);
    public const string ConsumeRetried = nameof(ConsumeRetried);
    public const string WorkerStopped = nameof(WorkerStopped);
    public const string RedeliveredToNewOwner = nameof(RedeliveredToNewOwner);
    public const string QueuedMessagesMayBeLost = nameof(QueuedMessagesMayBeLost);
}
