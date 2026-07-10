using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;
using Quartz;
using Quartz.Impl;
using Quartz.Impl.Matchers;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests;

public class RetrySchedulerTests
{
    private const string EXCHANGE = "orders";
    private const string QUEUE = "orders.handler";

    // in the future on purpose: Quartz fast-forwards a repeating trigger whose start is already in
    // the past (it does not replay missed repetitions), which would break the StartAt assertions.
    private static readonly DateTime SCHEDULED_AT = DateTime.UtcNow.AddDays(1);

    // A real in-memory Quartz scheduler, never started — ScheduleJob persists to the RAM store without firing.
    // A unique instance name keeps each test's scheduler isolated in the shared repository.
    private static ISchedulerFactory Factory()
        => new StdSchedulerFactory(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        });

    private static Dictionary<string, object?> Headers(Guid? messageId = null, int? retryCount = null, params (string Key, object? Value)[] extra)
    {
        Dictionary<string, object?> headers = [];

        if (messageId is Guid id) headers[TransportHeaders.MessageId] = id.ToByteArray();
        if (retryCount is int count) headers[TransportHeaders.RetryCount] = Encoding.UTF8.GetBytes(count.ToString());
        foreach ((string key, object? value) in extra) headers[key] = value;

        return headers;
    }

    [Fact]
    public async Task Schedule_WritesAJobKeyedByMessageIdAndRetryCount_GroupedByExchange()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 2), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(new JobKey($"{messageId:N}:2", EXCHANGE), TestContext.Current.CancellationToken);

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

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 2), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(new JobKey($"{messageId:N}:2", EXCHANGE), TestContext.Current.CancellationToken);

        Assert.NotNull(job);
        Assert.True(job.Durable);
    }

    [Fact]
    public async Task Schedule_DescribesTheJobAndTriggerWithTheQueue()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 1), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = new($"{messageId:N}:1", EXCHANGE);
        IJobDetail job = (await scheduler.GetJobDetail(key, TestContext.Current.CancellationToken))!;
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal(QUEUE, job.Description);
        Assert.Equal(QUEUE, trigger.Description);
    }

    [Fact]
    public async Task Schedule_TriggerStartsAtTheGivenUtcTime()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 1), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(new JobKey($"{messageId:N}:1", EXCHANGE), TestContext.Current.CancellationToken));

        Assert.Equal(new DateTimeOffset(SCHEDULED_AT), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_TriggerRepeatsEveryFiveMinutes_UpToMaxAttempts()
    {
        // the produce re-executions ARE the trigger's own repetitions — a failed produce has
        // nothing to schedule, and Quartz itself counts the fires and completes the trigger.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 1), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ISimpleTrigger trigger = Assert.IsAssignableFrom<ISimpleTrigger>(Assert.Single(await scheduler.GetTriggersOfJob(new JobKey($"{messageId:N}:1", EXCHANGE), TestContext.Current.CancellationToken)));

        Assert.Equal(TimeSpan.FromMinutes(5), trigger.RepeatInterval);
        Assert.Equal(4, trigger.RepeatCount);
    }

    [Fact]
    public async Task Schedule_WritesTheExchangeAndBodyIntoTheJobData()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "hello"u8.ToArray(), Headers(messageId, 0), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(new JobKey($"{messageId:N}:0", EXCHANGE), TestContext.Current.CancellationToken))!;

        Assert.Equal(EXCHANGE, job.JobDataMap.GetString(RetryJob.EXCHANGE_KEY));
        Assert.Equal("hello", Encoding.UTF8.GetString(Convert.FromBase64String(job.JobDataMap.GetString(RetryJob.BODY_KEY)!)));
    }

    [Fact]
    public async Task Schedule_RoundTripsHeaders_PreservingNull_AndNormalizingTypedValues()
    {
        // a foreign publisher's AMQP field table can carry typed values: a non-byte value travels
        // as the UTF-8 bytes of its invariant text — the same philosophy the transport decodes with.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            EXCHANGE, QUEUE, "body"u8.ToArray(),
            Headers(messageId, 0, ("k", "v"u8.ToArray()), ("n", null), ("s", "text"), ("i", 7)),
            SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(new JobKey($"{messageId:N}:0", EXCHANGE), TestContext.Current.CancellationToken))!;
        List<KeyValuePair<string, byte[]?>> decoded = JsonSerializer.Deserialize<List<KeyValuePair<string, byte[]?>>>(job.JobDataMap.GetString(RetryJob.HEADERS_KEY)!)!;

        Assert.Contains(decoded, header => header.Key == "k" && header.Value!.SequenceEqual("v"u8.ToArray()));
        Assert.Contains(decoded, header => header.Key == "n" && header.Value is null);
        Assert.Contains(decoded, header => header.Key == "s" && header.Value!.SequenceEqual("text"u8.ToArray()));
        Assert.Contains(decoded, header => header.Key == "i" && header.Value!.SequenceEqual("7"u8.ToArray()));
    }

    [Fact]
    public async Task Schedule_SameDeliveryTwice_ReparksTheSameJob_LastWriteWins()
    {
        // an at-least-once duplicate of the same failure must not throw nor duplicate the park: it
        // overwrites job and trigger, restarting the ladder from the newest scheduled time (which
        // also revives a dead-letter parked under the same key).
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();
        RetryScheduler sut = new(factory);

        await sut.Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 1), SCHEDULED_AT, TestContext.Current.CancellationToken);
        await sut.Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(messageId, 1), SCHEDULED_AT.AddMinutes(10), TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(EXCHANGE), TestContext.Current.CancellationToken));
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal($"{messageId:N}:1", key.Name);
        Assert.Equal(new DateTimeOffset(SCHEDULED_AT.AddMinutes(10)), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_NoMessageIdHeader_FallsBackToAFreshId()
    {
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(EXCHANGE, QUEUE, "body"u8.ToArray(), Headers(retryCount: 0), SCHEDULED_AT, TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(EXCHANGE), TestContext.Current.CancellationToken));

        Assert.Matches("^[0-9a-f]{32}:0$", key.Name);
    }
}
