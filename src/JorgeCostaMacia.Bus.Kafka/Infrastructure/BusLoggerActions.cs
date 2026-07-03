namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The outcome vocabulary stamped as the <c>Action</c> scope property on every outcome log — a
/// low-cardinality label, so the bus's failures are indexable and manageable from the log platform
/// (panels and alerts per outcome, no template matching).
/// </summary>
internal static class BusLoggerActions
{
    public const string RequeuedToRetry = nameof(RequeuedToRetry);
    public const string ScheduledToRetry = nameof(ScheduledToRetry);
    public const string ParkedToErrorTopic = nameof(ParkedToErrorTopic);
    public const string ProduceFailed = nameof(ProduceFailed);
    public const string ErrorProduceFailed = nameof(ErrorProduceFailed);
    public const string ScheduleFailed = nameof(ScheduleFailed);
    public const string RetrySchedulerMissing = nameof(RetrySchedulerMissing);
    public const string ConsumeCanceled = nameof(ConsumeCanceled);
    public const string ConsumeFailed = nameof(ConsumeFailed);
    public const string PartitionLost = nameof(PartitionLost);
    public const string FlushCanceled = nameof(FlushCanceled);
}
