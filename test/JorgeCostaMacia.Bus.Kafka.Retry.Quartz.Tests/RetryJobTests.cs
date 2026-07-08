using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Quartz;
using Quartz.Impl;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests;

public class RetryJobTests
{
    private readonly ProducerFake _producer = new();

    private Task Execute(JobDataMap data) => new RetryJob(_producer, NullLogger<RetryJob>.Instance).Execute(new JobExecutionContextFake(data));

    private static string EncodeHeaders(params (string Key, byte[]? Value)[] headers)
        => JsonSerializer.Serialize(headers.Select(header => new KeyValuePair<string, byte[]?>(header.Key, header.Value)));

    private static JobDataMap Data(string topic = "orders", byte[]? body = null, string? headers = null)
        => new()
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
    public async Task Execute_ProduceFails_SchedulesTheNextReexecution()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();
        _producer.Failure = new InvalidOperationException("boom");

        await Execute(Data(), job, trigger, scheduler);

        ITrigger reexecution = Assert.Single(await scheduler.GetTriggersOfJob(job.Key), e => e.Key.Group == "orders.error:1");
        Assert.Equal("1", reexecution.JobDataMap.GetString(RetryJob.REEXECUTION_KEY));
        Assert.True(reexecution.StartTimeUtc > DateTimeOffset.UtcNow.Add(RetryJob.REEXECUTION_DELAYS[0]).AddMinutes(-1));
    }

    [Fact]
    public async Task Execute_LadderExhausted_DeadLettersTheJobDurable()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();
        _producer.Failure = new InvalidOperationException("boom");
        JobDataMap data = Data();
        data[RetryJob.REEXECUTION_KEY] = RetryJob.REEXECUTION_DELAYS.Length.ToString();

        await Execute(data, job, trigger, scheduler);

        IJobDetail? parked = await scheduler.GetJobDetail(job.Key);
        Assert.NotNull(parked);
        Assert.True(parked.Durable);
        Assert.Contains(".fault:boom", parked.Description);
        Assert.DoesNotContain(await scheduler.GetTriggersOfJob(job.Key), e => e.Key.Group.Contains(".error:"));
    }

    [Fact]
    public async Task Execute_DurableJobSucceeds_DeletesItself()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed(durable: true);

        await Execute(Data(), job, trigger, scheduler);

        Assert.Single(_producer.Produced);
        Assert.Null(await scheduler.GetJobDetail(job.Key));
    }

    private Task Execute(JobDataMap data, IJobDetail job, ITrigger trigger, IScheduler scheduler)
        => new RetryJob(_producer, NullLogger<RetryJob>.Instance).Execute(new JobExecutionContextFake(data, job, trigger, scheduler));

    /// <summary>A real in-memory scheduler (never started) seeded with the job — durable alone, or with its one-shot trigger.</summary>
    private static async Task<(IScheduler Scheduler, IJobDetail Job, ITrigger Trigger)> Seed(bool durable = false)
    {
        IScheduler scheduler = await new StdSchedulerFactory(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        }).GetScheduler();

        JobBuilder builder = JobBuilder.Create<RetryJob>().WithIdentity("message-1:0", "orders");
        IJobDetail job = (durable ? builder.StoreDurably() : builder).Build();
        ITrigger trigger = TriggerBuilder.Create().ForJob(job).WithIdentity("message-1:0", "orders").StartAt(DateTimeOffset.UtcNow.AddMinutes(30)).Build();

        if (durable)
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
