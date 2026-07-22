using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;
using Quartz;
using Quartz.Impl;
using Quartz.Impl.Matchers;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests;

public class RetrySchedulerTests
{
    private const string Topic = "orders";
    private const string GroupId = "orders.handler";

    // in the future on purpose: Quartz fast-forwards a repeating trigger whose start is already in
    // the past (it does not replay missed repetitions), which would break the StartAt assertions.
    private static readonly DateTime ScheduledAt = DateTime.UtcNow.AddDays(1);

    // A real in-memory Quartz scheduler, never started — ScheduleJob persists to the RAM store without firing.
    // A unique instance name keeps each test's scheduler isolated in the shared repository.
    private static ISchedulerFactory Factory()
        => new StdSchedulerFactory(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        });

    private static Headers Headers(Guid? messageId = null, int? retryCount = null, params (string Key, byte[]? Value)[] extra)
    {
        Headers headers = new Headers();

        if (messageId is Guid id)
        {
            headers.Add(TransportHeaders.MessageId, id.ToByteArray());
        }

        if (retryCount is int count)
        {
            headers.Add(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(count.ToString()));
        }

        foreach ((string key, byte[]? value) in extra)
        {
            headers.Add(key, value);
        }

        return headers;
    }

    [Fact]
    public async Task Schedule_WritesAJobKeyedByMessageIdAndRetryCount_GroupedByTopic()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 2), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(new JobKey($"{messageId}:2", Topic), TestContext.Current.CancellationToken);

        Assert.NotNull(job);
        Assert.Equal(typeof(RetryJob), job.JobType);
        Assert.True(job.RequestsRecovery);
    }

    [Fact]
    public async Task Schedule_ParksTheJobDurable()
    {
        // durable from birth: when the last repetition fails, Quartz completes the trigger — the
        // durable job must stay parked in the store as the dead-letter instead of dying with it.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 2), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(new JobKey($"{messageId}:2", Topic), TestContext.Current.CancellationToken);

        Assert.NotNull(job);
        Assert.True(job.Durable);
    }

    [Fact]
    public async Task Schedule_DescribesTheJobAndTriggerWithTheGroupId()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 1), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = new JobKey($"{messageId}:1", Topic);
        IJobDetail job = (await scheduler.GetJobDetail(key, TestContext.Current.CancellationToken))!;
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal(GroupId, job.Description);
        Assert.Equal(GroupId, trigger.Description);
    }

    [Fact]
    public async Task Schedule_TriggerStartsAtTheGivenUtcTime()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 1), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(new JobKey($"{messageId}:1", Topic), TestContext.Current.CancellationToken));

        Assert.Equal(new DateTimeOffset(ScheduledAt), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_TriggerRepeatsEveryFiveMinutes_UpToMaxAttempts()
    {
        // the produce re-executions ARE the trigger's own repetitions — a failed produce has
        // nothing to schedule, and Quartz itself counts the fires and completes the trigger.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 1), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ISimpleTrigger trigger = Assert.IsAssignableFrom<ISimpleTrigger>(Assert.Single(await scheduler.GetTriggersOfJob(new JobKey($"{messageId}:1", Topic), TestContext.Current.CancellationToken)));

        Assert.Equal(TimeSpan.FromMinutes(5), trigger.RepeatInterval);
        Assert.Equal(4, trigger.RepeatCount);
    }

    [Fact]
    public async Task Schedule_WritesTheTopicAndBodyIntoTheJobData()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "hello"u8.ToArray(), Headers(messageId, 0), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(new JobKey($"{messageId}:0", Topic), TestContext.Current.CancellationToken))!;

        Assert.Equal(Topic, job.JobDataMap.GetString(RetryJob.TopicKey));
        Assert.Equal("hello", Encoding.UTF8.GetString(Convert.FromBase64String(job.JobDataMap.GetString(RetryJob.BodyKey)!)));
    }

    [Fact]
    public async Task Schedule_RoundTripsHeaders_PreservingNullAndDuplicates()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Topic, GroupId, "body"u8.ToArray(),
            Headers(messageId, 0, ("k", "v"u8.ToArray()), ("n", null), ("dup", "1"u8.ToArray()), ("dup", "2"u8.ToArray())),
            ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(new JobKey($"{messageId}:0", Topic), TestContext.Current.CancellationToken))!;
        List<KeyValuePair<string, byte[]?>> decoded = JsonSerializer.Deserialize<List<KeyValuePair<string, byte[]?>>>(job.JobDataMap.GetString(RetryJob.HeadersKey)!)!;

        Assert.Contains(decoded, header => header.Key == "k" && header.Value!.SequenceEqual("v"u8.ToArray()));
        Assert.Contains(decoded, header => header.Key == "n" && header.Value is null);
        Assert.Equal(2, decoded.Count(header => header.Key == "dup"));
    }

    [Fact]
    public async Task Schedule_SameDeliveryTwice_ReparksTheSameJob_LastWriteWins()
    {
        // an at-least-once duplicate of the same failure must not throw nor duplicate the park: it
        // overwrites job and trigger, restarting the ladder from the newest scheduled time (which
        // also revives a dead-letter parked under the same key).
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();
        RetryScheduler sut = new RetryScheduler(factory);

        await sut.Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 1), ScheduledAt, TestContext.Current.CancellationToken);
        await sut.Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(messageId, 1), ScheduledAt.AddMinutes(10), TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(Topic), TestContext.Current.CancellationToken));
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal($"{messageId}:1", key.Name);
        Assert.Equal(new DateTimeOffset(ScheduledAt.AddMinutes(10)), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_NoMessageIdHeader_FallsBackToAFreshId()
    {
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(Topic, GroupId, "body"u8.ToArray(), Headers(retryCount: 0), ScheduledAt, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(Topic), TestContext.Current.CancellationToken));

        Assert.Matches("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:0$", key.Name);
    }
}
