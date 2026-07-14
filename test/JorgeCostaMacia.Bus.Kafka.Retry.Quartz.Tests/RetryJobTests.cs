using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests.Fakes;
using Quartz;
using Quartz.Impl;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests;

public class RetryJobTests
{
    private readonly ProducerFake _producer = new ProducerFake();

    private static string EncodeHeaders(params (string Key, byte[]? Value)[] headers)
        => JsonSerializer.Serialize(headers.Select(header => new KeyValuePair<string, byte[]?>(header.Key, header.Value)));

    private static JobDataMap Data(string topic = "orders", byte[]? body = null, string? headers = null)
        => new JobDataMap()
        {
            [RetryJob.TOPIC_KEY] = topic,
            [RetryJob.BODY_KEY] = Convert.ToBase64String(body ?? "body"u8.ToArray()),
            [RetryJob.HEADERS_KEY] = headers ?? "[]"
        };

    [Fact]
    public async Task Execute_ProducesTheBodyToTheTopic()
    {
        await Execute(Data(topic: "orders", body: "hello"u8.ToArray()));

        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal("orders", topic);
        Assert.Equal("hello", Encoding.UTF8.GetString(message.Value));
    }

    [Fact]
    public async Task Execute_DecodesHeaders_PreservingNullAndDuplicates()
    {
        await Execute(Data(headers: EncodeHeaders(("k", "v"u8.ToArray()), ("n", null), ("dup", "1"u8.ToArray()), ("dup", "2"u8.ToArray()))));

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Equal("v", Encoding.UTF8.GetString(message.Headers.GetLastBytes("k")));
        Assert.Null(Assert.Single(message.Headers, header => header.Key == "n").GetValueBytes());
        Assert.Equal(2, message.Headers.Count(header => header.Key == "dup"));
    }

    [Fact]
    public async Task Execute_NoHeaders_ProducesEmptyHeaders()
    {
        await Execute(Data(headers: "[]"));

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Empty(message.Headers);
    }

    [Fact]
    public async Task Execute_MissingTopic_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.TOPIC_KEY);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Execute_MissingBody_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.BODY_KEY);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Execute_Succeeds_DeletesTheJob_PendingRepetitionsIncluded()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();

        await Execute(Data(), job, trigger, scheduler);

        Assert.Single(_producer.Produced);
        Assert.Null(await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken));
        Assert.Empty(await scheduler.GetTriggersOfJob(job.Key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Execute_ProduceFails_LetsTheFailureBubble_AndWritesNothing()
    {
        // the failure path is pure Quartz lifecycle: the exception bubbles (Quartz wraps it for the
        // job listeners) and the trigger repeats the produce on its own, so the job must leave the
        // store exactly as it found it — job, trigger and description intact.
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();
        _producer.Failure = new InvalidOperationException("boom");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Execute(Data(), job, trigger, scheduler));

        IJobDetail? parked = await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken);
        Assert.NotNull(parked);
        Assert.True(parked.Durable);
        Assert.Equal("orders.handler", parked.Description);
        Assert.Single(await scheduler.GetTriggersOfJob(job.Key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Execute_RefiredDeadLetterFails_StaysParked()
    {
        // a parked dead-letter (durable, trigger-less) re-fired with IScheduler.TriggerJob that
        // fails again must let the failure bubble and stay parked, untouched.
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed(parked: true);
        _producer.Failure = new InvalidOperationException("boom");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Execute(Data(), job, trigger, scheduler));

        IJobDetail? parked = await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken);
        Assert.NotNull(parked);
        Assert.True(parked.Durable);
    }

    [Fact]
    public async Task Execute_RefiredDeadLetterSucceeds_DeletesTheJob()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed(parked: true);

        await Execute(Data(), job, trigger, scheduler);

        Assert.Single(_producer.Produced);
        Assert.Null(await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken));
    }

    /// <summary>Fires the job over a freshly seeded scheduler — for tests that only exercise the produce.</summary>
    private async Task Execute(JobDataMap data)
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();

        await Execute(data, job, trigger, scheduler);
    }

    private Task Execute(JobDataMap data, IJobDetail job, ITrigger trigger, IScheduler scheduler)
        => new RetryJob(_producer).Execute(new JobExecutionContextFake(data, job, trigger, scheduler));

    /// <summary>
    /// A real in-memory scheduler (never started) seeded with the durable job — with its repeating
    /// trigger, or trigger-less like a parked dead-letter.
    /// </summary>
    private static async Task<(IScheduler Scheduler, IJobDetail Job, ITrigger Trigger)> Seed(bool parked = false)
    {
        IScheduler scheduler = await new StdSchedulerFactory(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        }).GetScheduler();

        IJobDetail job = JobBuilder.Create<RetryJob>()
            .WithIdentity("message-1:0", "orders")
            .WithDescription("orders.handler")
            .StoreDurably()
            .Build();

        ITrigger trigger = TriggerBuilder.Create()
            .ForJob(job)
            .WithIdentity("message-1:0", "orders")
            .StartAt(DateTimeOffset.UtcNow.AddMinutes(30))
            .WithSimpleSchedule(schedule => schedule.WithInterval(TimeSpan.FromMinutes(5)).WithRepeatCount(4))
            .Build();

        if (parked)
        {
            await scheduler.AddJob(job, replace: false);
        }
        else
        {
            await scheduler.ScheduleJob(job, trigger);
        }

        return (scheduler, job, trigger);
    }
}
