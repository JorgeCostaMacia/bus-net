using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class CommandErrorHandlerTests
{
    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();

    private Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> CommandError(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null, bool scheduler = true)
        => new(_producer, scheduler ? _scheduler : null, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID, intervals ?? [], excludes ?? []);

    [Fact]
    public async Task MissingRetryCountHeader_ReportsFaulted()
    {
        // an envelope whose retry count cannot be read is unreadable for the ladder: the handler
        // reports Faulted so the worker relays the delivery to the fault lane instead of retrying.
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.BareTransport(), new InvalidOperationException("boom"));

        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Faulted, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task NoLadder_ParksToErrorTopic()
    {
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException("boom"));

        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.error", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidOperationException).FullName, body.GetProperty("Error").GetProperty("Type").GetString());
        Assert.Equal("boom", body.GetProperty("Error").GetProperty("Message").GetString());
        Assert.Equal(Deliveries.GROUP_ID, body.GetProperty("GroupId").GetString());
        Assert.Equal(Deliveries.TOPIC, body.GetProperty("Topic").GetString());
        Assert.Equal(0, body.GetProperty("Partition").GetInt32());
        Assert.Equal(10, body.GetProperty("Offset").GetInt64());
        Assert.Equal("pepe", body.GetProperty("Message").GetProperty("Name").GetString());

        Assert.Equal(typeof(InvalidOperationException).FullName, Deliveries.Header(message, TransportHeaders.ErrorType));
        Assert.Equal(Deliveries.GROUP_ID, Deliveries.Header(message, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId), new InvalidOperationException());

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateId, out byte[] id) && new Guid(id) == aggregateId);
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateCorrelationId, out byte[] correlation) && new Guid(correlation) == aggregateCorrelationId);
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidOperationException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Deliveries.Transport(), failure);

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Message.Value);
        JsonElement error = body.GetProperty("Error");
        Assert.Equal("the real cause", error.GetProperty("InnerError").GetProperty("Message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("InnerError").GetProperty("Type").GetString());
        Assert.Equal("required", error.GetProperty("Data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("MachineName").GetString());
    }

    [Fact]
    public async Task ZeroInterval_RequeuesToTopicTail()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.TOPIC, topic);
        Assert.Equal("1", Deliveries.Header(message, TransportHeaders.RetryCount));
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task PositiveInterval_ParksThroughScheduler()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Scheduled, sut.Result);
        Assert.Empty(_producer.Produced);
        (string topic, string groupId, _, Headers headers, _) = Assert.Single(_scheduler.Scheduled);
        Assert.Equal(Deliveries.TOPIC, topic);
        Assert.Equal(Deliveries.GROUP_ID, groupId);
        Assert.True(headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] retry) && Encoding.UTF8.GetString(retry) == "1");
    }

    [Fact]
    public async Task LadderExhausted_Parks()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(retryCount: 2), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task ExcludedException_Parks_InheritanceAware()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero], [typeof(BaseFailure)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new DerivedFailure()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task PositiveInterval_WithoutScheduler_ParksAsTerminal()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)], scheduler: false);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.TOPIC}.error", Assert.Single(_producer.Produced).Topic);
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task SchedulerFails_LeavesUnhandled()
    {
        _scheduler.Failure = new InvalidOperationException("scheduler down");
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task RequeueProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task SecondRetry_ContinuesTheCumulativeCount()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Deliveries.Transport(retryCount: 1), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Deliveries.Header(Assert.Single(_producer.Produced).Message, TransportHeaders.RetryCount));
    }
}
