using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers.Commands;

public class CommandFaultHandlerTests
{
    private readonly ProducerFake _producer = new ProducerFake();

    private CommandFaultHandler<TestCommand, RecordingCommandHandler> Fault(ILogger? logger = null)
        => new CommandFaultHandler<TestCommand, RecordingCommandHandler>(_producer, logger ?? NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId);

    [Fact]
    public async Task ParksToFaultTopic_WithTheBodyAsText()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidCastException).FullName, body.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("bad header", body.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.GroupId, body.GetProperty("groupId").GetString());
        Assert.Equal("not json", body.GetProperty("message").GetString());
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidCastException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), failure);

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Message.Value);
        JsonElement error = body.GetProperty("error");
        Assert.Equal("the real cause", error.GetProperty("innerError").GetProperty("message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("innerError").GetProperty("type").GetString());
        Assert.Equal("required", error.GetProperty("data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("machineName").GetString());
    }

    [Fact]
    public async Task ParkedHeaders_StampTheFailure()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Equal(typeof(InvalidCastException).FullName, Deliveries.Header(message, TransportHeaders.ErrorType));
        Assert.Equal("bad header", Deliveries.Header(message, TransportHeaders.ErrorMessage));
        Assert.Equal(Deliveries.GroupId, Deliveries.Header(message, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(CommandFaultContext.Create("{}"u8.ToArray(), Deliveries.Transport(), new InvalidCastException()), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task ShutdownWhileParking_LeavesUnhandled_Silently()
    {
        // a deploy cancels the token while the fault is being parked. The outcome matches a broker
        // failure — Unhandled, so the delivery is redelivered rather than acked away — but it must get
        // there silently: the generic catch logs "Parking failed." at Error, and a routine shutdown
        // must not look like a parking outage.
        RecordingLogger<CommandFaultHandlerTests> logger = new RecordingLogger<CommandFaultHandlerTests>();
        _producer.Failure = new OperationCanceledException();
        CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault(logger);

        await sut.Handle(CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header")), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
        Assert.DoesNotContain(logger.Logged, entry => entry.Level >= LogLevel.Error);
    }
}
