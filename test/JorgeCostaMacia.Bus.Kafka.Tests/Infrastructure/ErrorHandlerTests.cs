using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ErrorHandlerTests
{
    private const string TOPIC = "orders";
    private const string GROUP_ID = "orders.handler";

    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();

    private Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> CommandError(ImmutableList<TimeSpan>? intervals = null, ImmutableList<Type>? excludes = null, bool scheduler = true)
        => new(_producer, scheduler ? _scheduler : null, NullLogger.Instance, TOPIC, GROUP_ID, intervals ?? [], excludes ?? []);

    private Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> EventError(ImmutableList<TimeSpan>? intervals = null)
        => new(_producer, _scheduler, NullLogger.Instance, TOPIC, GROUP_ID, intervals ?? [], []);

    private Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> Fault() => new(_producer, NullLogger.Instance, TOPIC, GROUP_ID);

    private static Transport Transport(int retryCount = 0, Guid? aggregateId = null, Guid? aggregateCorrelationId = null)
    {
        Headers headers =
        [
            new Header(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes(retryCount.ToString())),
            new Header(TransportHeaders.AggregateId, (aggregateId ?? Guid.NewGuid()).ToByteArray()),
            new Header(TransportHeaders.AggregateCorrelationId, (aggregateCorrelationId ?? Guid.NewGuid()).ToByteArray())
        ];

        return Domain.Transport.Create(new ConsumeResult<Ignore, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(TOPIC, new Partition(0), new Offset(10)),
            Message = new Message<Ignore, byte[]> { Value = "{}"u8.ToArray(), Headers = headers }
        });
    }

    private static string? Header(Message<Null, byte[]> message, string key)
        => message.Headers.TryGetLastBytes(key, out byte[] value) ? Encoding.UTF8.GetString(value) : null;

    [Fact]
    public async Task Command_NoLadder_ParksToErrorTopic()
    {
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Transport(), new InvalidOperationException("boom"));

        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.error", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidOperationException).FullName, body.GetProperty("ErrorType").GetString());
        Assert.Equal("boom", body.GetProperty("ErrorMessage").GetString());
        Assert.Equal(GROUP_ID, body.GetProperty("GroupId").GetString());
        Assert.Equal(TOPIC, body.GetProperty("Topic").GetString());
        Assert.Equal(0, body.GetProperty("Partition").GetInt32());
        Assert.Equal(10, body.GetProperty("Offset").GetInt64());
        Assert.Equal("pepe", body.GetProperty("Message").GetProperty("Name").GetString());

        Assert.Equal(typeof(InvalidOperationException).FullName, Header(message, TransportHeaders.ErrorType));
        Assert.Equal(GROUP_ID, Header(message, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task Command_ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        CommandErrorContext<TestCommand> context = new(new TestCommand("pepe"), Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId), new InvalidOperationException());

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateId, out byte[] id) && new Guid(id) == aggregateId);
        Assert.True(message.Headers.TryGetLastBytes(TransportHeaders.AggregateCorrelationId, out byte[] correlation) && new Guid(correlation) == aggregateCorrelationId);
    }

    [Fact]
    public async Task Command_ZeroInterval_RequeuesToTopicTail()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal(TOPIC, topic);
        Assert.Equal("1", Header(message, TransportHeaders.RetryCount));
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task Command_PositiveInterval_ParksThroughScheduler()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Scheduled, sut.Result);
        Assert.Empty(_producer.Produced);
        (string topic, _, Headers headers, _) = Assert.Single(_scheduler.Scheduled);
        Assert.Equal(TOPIC, topic);
        Assert.True(headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] retry) && Encoding.UTF8.GetString(retry) == "1");
    }

    [Fact]
    public async Task Command_LadderExhausted_Parks()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(retryCount: 2), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task Command_ExcludedException_Parks_InheritanceAware()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero], [typeof(BaseFailure)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new DerivedFailure()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
    }

    [Fact]
    public async Task Command_PositiveInterval_WithoutScheduler_ParksAsTerminal()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)], scheduler: false);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{TOPIC}.error", Assert.Single(_producer.Produced).Topic);
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task Command_SchedulerFails_LeavesUnhandled()
    {
        _scheduler.Failure = new InvalidOperationException("scheduler down");
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.FromMinutes(5)]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Command_RequeueProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task Command_SecondRetry_ContinuesTheCumulativeCount()
    {
        Infrastructure.Consumers.Commands.CommandErrorHandler<TestCommand, RecordingCommandHandler> sut = CommandError([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        await sut.Handle(new CommandErrorContext<TestCommand>(new TestCommand("pepe"), Transport(retryCount: 1), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Header(Assert.Single(_producer.Produced).Message, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task Event_ZeroInterval_RetargetsTheRetryToItsGroup()
    {
        Infrastructure.Consumers.Events.EventErrorHandler<TestEvent, TestEventSubscriber> sut = EventError([TimeSpan.Zero]);

        await sut.Handle(new EventErrorContext<TestEvent>(new TestEvent("pepe"), Transport(), new InvalidOperationException()), TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal(TOPIC, topic);
        Assert.Equal(GROUP_ID, Header(message, TransportHeaders.AggregateConsumers));
    }

    [Fact]
    public async Task Fault_ParksToFaultTopic_WithTheBodyAsText()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Transport(), new InvalidCastException("bad header"));

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.fault", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidCastException).FullName, body.GetProperty("ErrorType").GetString());
        Assert.Equal("bad header", body.GetProperty("ErrorMessage").GetString());
        Assert.Equal(GROUP_ID, body.GetProperty("GroupId").GetString());
        Assert.Equal("not json", body.GetProperty("Message").GetString());
    }

    [Fact]
    public async Task Fault_ProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(CommandFaultContext.Create("{}"u8.ToArray(), Transport(), new InvalidCastException()), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }
}
