using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class EventFaultHandlerTests
{
    private readonly ProducerFake _producer = new();

    private Infrastructure.Consumers.Events.EventFaultHandler<TestEvent, TestEventSubscriber> Fault()
        => new(_producer, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID);

    [Fact]
    public async Task ParksToFaultTopic_WithTheBodyAsText()
    {
        EventFaultContext context = EventFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

        Infrastructure.Consumers.Events.EventFaultHandler<TestEvent, TestEventSubscriber> sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(message.Value);
        Assert.Equal(typeof(InvalidCastException).FullName, body.GetProperty("Error").GetProperty("Type").GetString());
        Assert.Equal("bad header", body.GetProperty("Error").GetProperty("Message").GetString());
        Assert.Equal(Deliveries.GROUP_ID, body.GetProperty("GroupId").GetString());
        Assert.Equal("not json", body.GetProperty("Message").GetString());
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidCastException("outer", new FormatException("the real cause"));
        failure.Data["field"] = "required";
        EventFaultContext context = EventFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), failure);

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Message.Value);
        JsonElement error = body.GetProperty("Error");
        Assert.Equal("the real cause", error.GetProperty("InnerError").GetProperty("Message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("InnerError").GetProperty("Type").GetString());
        Assert.Equal("required", error.GetProperty("Data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("MachineName").GetString());
    }

    [Fact]
    public async Task ParkedHeaders_StampTheFailure()
    {
        EventFaultContext context = EventFaultContext.Create("not json"u8.ToArray(), Deliveries.Transport(), new InvalidCastException("bad header"));

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

        Infrastructure.Consumers.Events.EventFaultHandler<TestEvent, TestEventSubscriber> sut = Fault();
        await sut.Handle(EventFaultContext.Create("{}"u8.ToArray(), Deliveries.Transport(), new InvalidCastException()), TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }
}
