using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Events;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers.Events;

public class EventErrorHandlerTests
{
    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RetrySchedulerFake _scheduler = new RetrySchedulerFake();

    private EventErrorHandler<TestEvent, TestEventSubscriber> EventError(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null, bool scheduler = true)
        => new EventErrorHandler<TestEvent, TestEventSubscriber>(_producer, scheduler ? _scheduler : null, NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId, intervals ?? ImmutableList<TimeSpan>.Empty, excludes ?? ImmutableList<Type>.Empty);

    [Fact]
    public async Task MissingRetryCountHeader_ReportsFaulted()
    {
        // an envelope whose retry count cannot be read is unreadable for the ladder: the handler
        // reports Faulted so the worker relays the delivery to the fault lane instead of retrying.
        EventErrorContext<TestEvent> context = new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.BareTransport(), new InvalidOperationException("boom"));

        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Faulted, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task NoLadder_ParksToErrorTopic()
    {
        EventErrorContext<TestEvent> context = new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException("boom"));

        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.error", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidOperationException).FullName, body.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("boom", body.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.GroupId, body.GetProperty("groupId").GetString());
        Assert.Equal(Deliveries.Topic, body.GetProperty("topic").GetString());
        Assert.Equal(0, body.GetProperty("partition").GetInt32());
        Assert.Equal(10, body.GetProperty("offset").GetInt64());
        Assert.Equal("pepe", body.GetProperty("message").GetProperty("name").GetString());

        Assert.Equal(typeof(InvalidOperationException).FullName, Deliveries.Header(message, TransportHeaders.ErrorType));
        Assert.Equal(Deliveries.GroupId, Deliveries.Header(message, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        EventErrorContext<TestEvent> context = new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId), new InvalidOperationException());

        await EventError().Handle(context, TestContext.Current.CancellationToken);

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateId, out byte[] id) && new Guid(id) == aggregateId);
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateCorrelationId, out byte[] correlation) && new Guid(correlation) == aggregateCorrelationId);
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidOperationException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        EventErrorContext<TestEvent> context = new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), failure);

        await EventError().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Message.Value);
        JsonElement error = body.GetProperty("error");
        Assert.Equal("the real cause", error.GetProperty("innerError").GetProperty("message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("innerError").GetProperty("type").GetString());
        Assert.Equal("required", error.GetProperty("data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("machineName").GetString());
    }

    [Fact]
    public async Task ZeroInterval_RequeuesToTopicTail_RetargetedToItsGroup()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.Zero));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.Topic, topic);
        Assert.Equal("1", Deliveries.Header(message, TransportHeaders.RetryCount));
        Assert.Equal(Deliveries.GroupId, Deliveries.Header(message, TransportHeaders.AggregateConsumers));
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task PositiveInterval_ParksThroughScheduler_RetargetedToItsGroup()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.FromMinutes(5)));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Scheduled, sut.Result);
        Assert.Empty(_producer.Produced);
        (string topic, string groupId, _, Headers headers, _) = Assert.Single(_scheduler.Scheduled);
        Assert.Equal(Deliveries.Topic, topic);
        Assert.Equal(Deliveries.GroupId, groupId);
        Assert.True(headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] retry) && Encoding.UTF8.GetString(retry) == "1");
        Assert.True(headers.TryGetLastBytes(TransportHeaders.AggregateConsumers, out byte[] consumers) && Encoding.UTF8.GetString(consumers) == Deliveries.GroupId);
    }

    [Fact]
    public async Task LadderExhausted_Parks()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.Zero, TimeSpan.Zero));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(retryCount: 2), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.Topic}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task ExcludedException_Parks_InheritanceAware()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.Zero), ImmutableList.Create(typeof(BaseFailure)));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new DerivedFailure()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.Topic}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task PositiveInterval_WithoutScheduler_ParksAsTerminal()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.FromMinutes(5)), scheduler: false);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.Topic}.error", Assert.Single(_producer.Produced).Topic);
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task SchedulerFails_LeavesUnhandled()
    {
        _scheduler.Failure = new InvalidOperationException("scheduler down");
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.FromMinutes(5)));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task RequeueProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.Zero));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task SecondRetry_ContinuesTheCumulativeCount()
    {
        EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError(ImmutableList.Create(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero));

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(retryCount: 1), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Deliveries.Header(Assert.Single(_producer.Produced).Message, TransportHeaders.RetryCount));
    }
}
