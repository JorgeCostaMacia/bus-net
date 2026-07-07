using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class EventErrorHandlerTests
{
    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private sealed class BrokerFailure() : RabbitMQClientException("broker down");

    private readonly ProducerFake _producer = new();

    private Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> EventError(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null)
        => new(_producer, NullLogger.Instance, Deliveries.EXCHANGE, Deliveries.QUEUE, intervals ?? [], excludes ?? []);

    [Fact]
    public async Task NoLadder_ParksToErrorQueue()
    {
        EventErrorContext<TestEvent> context = new(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException("boom"));

        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, byte[] body, IReadOnlyDictionary<string, object?> headers) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);

        JsonElement parked = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(typeof(InvalidOperationException).FullName, parked.GetProperty("Error").GetProperty("Type").GetString());
        Assert.Equal("boom", parked.GetProperty("Error").GetProperty("Message").GetString());
        Assert.Equal(Deliveries.QUEUE, parked.GetProperty("Queue").GetString());
        Assert.Equal(Deliveries.EXCHANGE, parked.GetProperty("Exchange").GetString());
        Assert.Equal(10ul, parked.GetProperty("DeliveryTag").GetUInt64());
        Assert.Equal("pepe", parked.GetProperty("Message").GetProperty("Name").GetString());

        Assert.Equal(typeof(InvalidOperationException).FullName, Deliveries.Header(headers, TransportHeaders.ErrorType));
        Assert.Equal(Deliveries.QUEUE, Deliveries.Header(headers, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        EventErrorContext<TestEvent> context = new(new TestEvent("pepe"), Deliveries.Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId), new InvalidOperationException());

        await EventError().Handle(context, TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, object?> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal(aggregateId, new Guid((byte[])headers[TransportHeaders.AggregateId]!));
        Assert.Equal(aggregateCorrelationId, new Guid((byte[])headers[TransportHeaders.AggregateCorrelationId]!));
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidOperationException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        EventErrorContext<TestEvent> context = new(new TestEvent("pepe"), Deliveries.Transport(), failure);

        await EventError().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);
        JsonElement error = body.GetProperty("Error");
        Assert.Equal("the real cause", error.GetProperty("InnerError").GetProperty("Message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("InnerError").GetProperty("Type").GetString());
        Assert.Equal("required", error.GetProperty("Data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("MachineName").GetString());
    }

    [Fact]
    public async Task ZeroInterval_RepublishesToTheExchange()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string exchange, string routingKey, _, IReadOnlyDictionary<string, object?> headers) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.EXCHANGE, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal("1", Deliveries.Header(headers, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task PositiveInterval_ParksAsTerminal_WithNoScheduler()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);
    }

    [Fact]
    public async Task LadderExhausted_Parks()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(retryCount: 2), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.QUEUE}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task ExcludedException_Parks_InheritanceAware()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero], [typeof(BaseFailure)]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new DerivedFailure()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.QUEUE}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task RequeueBrokerFailure_LeavesUnhandled()
    {
        _producer.Failure = new BrokerFailure();
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task RequeueUnexpectedError_LeavesFaulted()
    {
        _producer.Failure = new InvalidOperationException("unexpected");
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Faulted, sut.Result);
    }

    [Fact]
    public async Task SecondRetry_ContinuesTheCumulativeCount()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Deliveries.Transport(retryCount: 1), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Deliveries.Header(Assert.Single(_producer.Produced).Headers, TransportHeaders.RetryCount));
    }
}
