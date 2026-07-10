using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class CommandFaultHandlerTests
{
    private readonly ProducerFake _producer = new();

    private Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> Fault()
        => new(_producer, NullLogger.Instance, Deliveries.QUEUE);

    [Fact]
    public async Task ParksToFaultQueue_WithTheBodyAsText()
    {
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string exchange, string routingKey, byte[] body, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.fault", routingKey);

        JsonElement parked = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(typeof(InvalidCastException).FullName, parked.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("bad header", parked.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.QUEUE, parked.GetProperty("queue").GetString());
        Assert.Equal("not json", parked.GetProperty("message").GetString());
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidCastException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        CommandFaultContext context = CommandFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), failure);

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);
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

        IReadOnlyDictionary<string, object?> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal(typeof(InvalidCastException).FullName, Deliveries.Header(headers, TransportHeaders.ErrorType));
        Assert.Equal("bad header", Deliveries.Header(headers, TransportHeaders.ErrorMessage));
        Assert.Equal(Deliveries.QUEUE, Deliveries.Header(headers, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new InvalidOperationException("broker down");

        Infrastructure.Consumers.Commands.CommandFaultHandler<TestCommand, RecordingCommandHandler> sut = Fault();
        await sut.Handle(CommandFaultContext.Create("{}"u8.ToArray(), Deliveries.Transport(), new InvalidCastException()), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }
}
