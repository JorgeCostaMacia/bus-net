using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class CommandFaultHandlerTests
{
    private readonly ProducerFake _producer = new();

    private Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> Fault()
        => new(_producer, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID);

    [Fact]
    public async Task ParksToFaultTopic_WithTheBodyAsText()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidCastException).FullName, body.GetProperty("ErrorType").GetString());
        Assert.Equal("bad header", body.GetProperty("ErrorMessage").GetString());
        Assert.Equal(Deliveries.GROUP_ID, body.GetProperty("GroupId").GetString());
        Assert.Equal("not json", body.GetProperty("Message").GetString());
    }

    [Fact]
    public async Task ParkedHeaders_StampTheFailure()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Equal(typeof(InvalidCastException).FullName, Deliveries.Header(message, TransportHeaders.ErrorType));
        Assert.Equal("bad header", Deliveries.Header(message, TransportHeaders.ErrorMessage));
        Assert.Equal(Deliveries.GROUP_ID, Deliveries.Header(message, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(CommandFaultContext.Create("{}"u8.ToArray(), Deliveries.Transport(), new InvalidCastException()), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }
}
