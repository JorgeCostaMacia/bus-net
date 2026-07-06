using Quartz;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests.Fakes;

/// <summary>
/// A minimal <see cref="IJobExecutionContext"/> for firing a job in a test: it exposes the merged job
/// data map and a cancellation token (all the retry job reads). Every other member throws, so a test
/// that depends on more surface fails loudly instead of silently.
/// </summary>
internal sealed class JobExecutionContextFake : IJobExecutionContext
{
    public JobExecutionContextFake(JobDataMap mergedJobDataMap) => MergedJobDataMap = mergedJobDataMap;

    public JobDataMap MergedJobDataMap { get; }

    public CancellationToken CancellationToken => CancellationToken.None;

    public IScheduler Scheduler => throw new NotSupportedException();
    public ITrigger Trigger => throw new NotSupportedException();
    public ICalendar? Calendar => throw new NotSupportedException();
    public bool Recovering => throw new NotSupportedException();
    public TriggerKey RecoveringTriggerKey => throw new NotSupportedException();
    public int RefireCount => throw new NotSupportedException();
    public IJobDetail JobDetail => throw new NotSupportedException();
    public IJob JobInstance => throw new NotSupportedException();
    public DateTimeOffset FireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? ScheduledFireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? PreviousFireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? NextFireTimeUtc => throw new NotSupportedException();
    public string FireInstanceId => throw new NotSupportedException();
    public object? Result { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public TimeSpan JobRunTime => throw new NotSupportedException();

    public object? Get(object key) => throw new NotSupportedException();
    public void Put(object key, object objectValue) => throw new NotSupportedException();
}
