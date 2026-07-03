using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using KafkaBus = JorgeCostaMacia.Bus.Kafka.Infrastructure.Bus;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ConsumerErrorTests
{
    private const string TOPIC = "orders";
    private const string GROUP_ID = "orders.handler";

    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();

    private ConsumerError CreateSut(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null, bool scheduler = true)
        => new(
            new KafkaBus(_producer, new Dictionary<Type, string>(), NullLogger<KafkaBus>.Instance),
            scheduler ? _scheduler : null,
            NullLogger.Instance,
            TOPIC,
            GROUP_ID,
            intervals ?? [],
            excludes ?? []);

    private static ConsumeResult<Null, byte[]> Delivery(int? retryCount = 0, byte[]? body = null)
    {
        Headers headers = [];

        if (retryCount is not null) headers.Add(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retryCount.Value.ToString()));

        return new ConsumeResult<Null, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(TOPIC, new Partition(0), new Offset(10)),
            Message = new Message<Null, byte[]> { Value = body ?? "{}"u8.ToArray(), Headers = headers }
        };
    }

    private static string? Header(Message<Null, byte[]> message, string key)
        => message.Headers.TryGetLastBytes(key, out byte[] value) ? Encoding.UTF8.GetString(value) : null;

    [Fact]
    public async Task Handle_NoLadder_ParksToErrorTopic()
    {
        bool acked = await CreateSut().Handle(Delivery(), new InvalidOperationException("boom"), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.error", topic);
        Assert.Equal(typeof(InvalidOperationException).FullName, Header(message, TransportHeaders.ErrorType));
        Assert.Equal("boom", Header(message, TransportHeaders.ErrorMessage));
        Assert.Equal(GROUP_ID, Header(message, TransportHeaders.ErrorGroupId));
        Assert.NotNull(Header(message, TransportHeaders.ErrorOccurredAt));
    }

    [Fact]
    public async Task Handle_MissingRetryHeader_ParksToErrorTopic()
    {
        bool acked = await CreateSut([TimeSpan.Zero]).Handle(Delivery(retryCount: null), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task Handle_LadderExhausted_ParksToErrorTopic()
    {
        bool acked = await CreateSut([TimeSpan.Zero, TimeSpan.Zero]).Handle(Delivery(retryCount: 2), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task Handle_ExcludedExceptionType_ParksToErrorTopic_InheritanceAware()
    {
        bool acked = await CreateSut([TimeSpan.Zero], [typeof(BaseFailure)]).Handle(Delivery(), new DerivedFailure(), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task Handle_ZeroInterval_RequeuesToTopicTail()
    {
        bool acked = await CreateSut([TimeSpan.Zero]).Handle(Delivery(body: "body"u8.ToArray()), new InvalidOperationException(), headers => headers.Add("targeted", "yes"u8.ToArray()), TestContext.Current.CancellationToken);

        Assert.True(acked);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal(TOPIC, topic);
        Assert.Equal("body"u8.ToArray(), message.Value);
        Assert.Equal("1", Header(message, TransportHeaders.RetryCount));
        Assert.Equal("yes", Header(message, "targeted"));
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task Handle_PositiveInterval_ParksThroughScheduler()
    {
        DateTime before = DateTime.UtcNow;

        bool acked = await CreateSut([TimeSpan.FromMinutes(5)]).Handle(Delivery(body: "body"u8.ToArray()), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        Assert.Empty(_producer.Produced);
        (string topic, byte[] body, Headers headers, DateTime scheduledAt) = Assert.Single(_scheduler.Scheduled);
        Assert.Equal(TOPIC, topic);
        Assert.Equal("body"u8.ToArray(), body);
        Assert.True(headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] retry) && Encoding.UTF8.GetString(retry) == "1");
        Assert.InRange(scheduledAt, before.AddMinutes(5), DateTime.UtcNow.AddMinutes(5));
    }

    [Fact]
    public async Task Handle_PositiveInterval_WithoutScheduler_LeavesUnacked()
    {
        bool acked = await CreateSut([TimeSpan.FromMinutes(5)], scheduler: false).Handle(Delivery(), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.False(acked);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Handle_SchedulerFails_LeavesUnacked()
    {
        _scheduler.Failure = new InvalidOperationException("scheduler down");

        bool acked = await CreateSut([TimeSpan.FromMinutes(5)]).Handle(Delivery(), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.False(acked);
    }

    [Fact]
    public async Task Handle_RequeueProduceFails_LeavesUnacked()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        bool acked = await CreateSut([TimeSpan.Zero]).Handle(Delivery(), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.False(acked);
    }

    [Fact]
    public async Task Handle_SecondRetry_ContinuesTheCumulativeCount()
    {
        bool acked = await CreateSut([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]).Handle(Delivery(retryCount: 1), new InvalidOperationException(), _ => { }, TestContext.Current.CancellationToken);

        Assert.True(acked);
        Assert.Equal("2", Header(Assert.Single(_producer.Produced).Message, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task Malformed_ParksToErrorTopic_WithTheOriginalEnvelope()
    {
        ConsumeResult<Null, byte[]> delivery = Delivery(retryCount: 3, body: "not json"u8.ToArray());

        bool acked = await CreateSut([TimeSpan.Zero]).Malformed(delivery, new InvalidCastException("bad header"), TestContext.Current.CancellationToken);

        Assert.True(acked);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.error", topic);
        Assert.Equal("not json"u8.ToArray(), message.Value);
        Assert.Equal("3", Header(message, TransportHeaders.RetryCount));
        Assert.Equal(typeof(InvalidCastException).FullName, Header(message, TransportHeaders.ErrorType));
    }

    [Fact]
    public async Task Malformed_ErrorProduceFails_LeavesUnacked()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        bool acked = await CreateSut().Malformed(Delivery(), new InvalidCastException(), TestContext.Current.CancellationToken);

        Assert.False(acked);
    }

    [Fact]
    public async Task Handle_Requeue_DoesNotMutateTheOriginalDelivery()
    {
        ConsumeResult<Null, byte[]> delivery = Delivery();

        await CreateSut([TimeSpan.Zero]).Handle(delivery, new InvalidOperationException(), headers => headers.Add("targeted", "yes"u8.ToArray()), TestContext.Current.CancellationToken);

        Assert.True(delivery.Message.Headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] original));
        Assert.Equal("0", Encoding.UTF8.GetString(original));
        Assert.False(delivery.Message.Headers.TryGetLastBytes("targeted", out _));
    }
}
