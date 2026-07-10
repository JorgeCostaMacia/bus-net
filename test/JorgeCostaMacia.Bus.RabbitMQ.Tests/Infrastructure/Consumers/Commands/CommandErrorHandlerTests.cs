using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class CommandErrorHandlerTests
{
    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private sealed class BrokerFailure() : RabbitMQClientException("broker down");

    private readonly ProducerFake _producer = new();

    private Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> CommandError(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null)
        => new(_producer, NullLogger.Instance, Deliveries.EXCHANGE, Deliveries.QUEUE, intervals ?? [], excludes ?? []);

    [Fact]
    public async Task NoLadder_ParksToErrorQueue()
    {
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException("boom"));

        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, byte[] body, IReadOnlyDictionary<string, object?> headers) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);

        JsonElement parked = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(typeof(InvalidOperationException).FullName, parked.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("boom", parked.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.QUEUE, parked.GetProperty("queue").GetString());
        Assert.Equal(Deliveries.EXCHANGE, parked.GetProperty("exchange").GetString());
        Assert.Equal(10ul, parked.GetProperty("deliveryTag").GetUInt64());
        Assert.Equal("pepe", parked.GetProperty("message").GetProperty("name").GetString());

        Assert.Equal(typeof(InvalidOperationException).FullName, Deliveries.Header(headers, TransportHeaders.ErrorType));
        Assert.Equal(Deliveries.QUEUE, Deliveries.Header(headers, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId), new InvalidOperationException());

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, object?> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal(aggregateId, new Guid((byte[])headers[TransportHeaders.AggregateId]!));
        Assert.Equal(aggregateCorrelationId, new Guid((byte[])headers[TransportHeaders.AggregateCorrelationId]!));
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidOperationException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(), failure);

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);
        JsonElement error = body.GetProperty("error");
        Assert.Equal("the real cause", error.GetProperty("innerError").GetProperty("message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("innerError").GetProperty("type").GetString());
        Assert.Equal("required", error.GetProperty("data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("machineName").GetString());
    }

    [Fact]
    public async Task ZeroInterval_RepublishesToTheExchange()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string exchange, string routingKey, _, IReadOnlyDictionary<string, object?> headers) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.EXCHANGE, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal("1", Deliveries.Header(headers, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task PositiveInterval_ParksAsTerminal_WithNoScheduler()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);
    }

    [Fact]
    public async Task LadderExhausted_Parks()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(retryCount: 2), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.QUEUE}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task ExcludedException_Parks_InheritanceAware()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero], [typeof(BaseFailure)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new DerivedFailure()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.QUEUE}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task RequeueBrokerFailure_LeavesUnhandled()
    {
        _producer.Failure = new BrokerFailure();
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task RequeueUnexpectedError_LeavesFaulted()
    {
        _producer.Failure = new InvalidOperationException("unexpected");
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Faulted, sut.Result);
    }

    [Fact]
    public async Task SecondRetry_ContinuesTheCumulativeCount()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(retryCount: 1), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Deliveries.Header(Assert.Single(_producer.Produced).Headers, TransportHeaders.RetryCount));
    }
}
