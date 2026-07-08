using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using KafkaBus = JorgeCostaMacia.Bus.Kafka.Infrastructure.Bus;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class BusTests
{
    private readonly ProducerFake _producer = new();

    private KafkaBus CreateSut(params (Type Type, string Topic)[] messages)
        => new(_producer, messages.ToDictionary(e => e.Type, e => e.Topic));

    private static string? Header(Message<Null, byte[]> message, string key)
        => message.Headers.TryGetLastBytes(key, out byte[] value) ? Encoding.UTF8.GetString(value) : null;

    private static Guid GuidHeader(Message<Null, byte[]> message, string key)
    {
        Assert.True(message.Headers.TryGetLastBytes(key, out byte[] value));

        return new Guid(value);
    }

    [Fact]
    public async Task Send_UnmappedType_Throws()
    {
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateSut().Send(new TestCommand("pepe"), TestContext.Current.CancellationToken));

        Assert.Contains(typeof(TestCommand).FullName!, exception.Message);
    }

    [Fact]
    public async Task Send_Fresh_BuildsANewEnvelope()
    {
        TestCommand command = new("pepe", aggregateConsumers: ["g1", "g2"]);

        await CreateSut((typeof(TestCommand), "orders")).Send(command, TestContext.Current.CancellationToken);

        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal("orders", topic);
        Assert.Equal(typeof(TestCommand).FullName, Header(message, TransportHeaders.MessageType));
        Assert.Equal("orders", Header(message, TransportHeaders.MessageDestinationAddress));
        Assert.Null(Header(message, TransportHeaders.MessageOriginAddress));
        Assert.Equal("orders", Header(message, TransportHeaders.ConversationAddress));
        Assert.Equal(GuidHeader(message, TransportHeaders.MessageId), GuidHeader(message, TransportHeaders.ConversationId));
        Assert.Equal(command.AggregateId, GuidHeader(message, TransportHeaders.AggregateId));
        Assert.Equal(command.AggregateCorrelationId, GuidHeader(message, TransportHeaders.AggregateCorrelationId));
        Assert.Equal("g1,g2", Header(message, TransportHeaders.AggregateConsumers));
        Assert.Equal("0", Header(message, TransportHeaders.RetryCount));
        Assert.NotNull(Header(message, TransportHeaders.MessageTypeUrn));
        Assert.Contains("pepe", Encoding.UTF8.GetString(message.Value));
    }

    [Fact]
    public async Task Send_WithTransport_ContinuesTheConversation()
    {
        Guid inboundMessageId = Guid.NewGuid();
        Guid conversationId = Guid.NewGuid();

        Headers inbound = new()
        {
            { TransportHeaders.MessageId, inboundMessageId.ToByteArray() },
            { TransportHeaders.MessageType, "Inbound"u8.ToArray() },
            { TransportHeaders.MessageTypeUrn, "urn:message:Inbound"u8.ToArray() },
            { TransportHeaders.MessageDestinationAddress, "orders"u8.ToArray() },
            { TransportHeaders.MessageOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
            { TransportHeaders.ConversationId, conversationId.ToByteArray() },
            { TransportHeaders.ConversationAddress, "orders"u8.ToArray() },
            { TransportHeaders.ConversationOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
            { TransportHeaders.AggregateId, Guid.NewGuid().ToByteArray() },
            { TransportHeaders.AggregateCorrelationId, Guid.NewGuid().ToByteArray() },
            { TransportHeaders.AggregateOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
            { TransportHeaders.AggregateConsumers, "old"u8.ToArray() },
            { TransportHeaders.RetryCount, "2"u8.ToArray() }
        };

        Transport transport = new(inbound.ToImmutableList(), "orders", new Partition(0), new Offset(10), null, new Timestamp(DateTime.UtcNow));
        TestEvent message = new("pepe", aggregateConsumers: ["g1"]);

        await CreateSut((typeof(TestEvent), "payments")).Publish(message, transport, TestContext.Current.CancellationToken);

        (string topic, Message<Null, byte[]> produced) = Assert.Single(_producer.Produced);
        Assert.Equal("payments", topic);
        Assert.Equal(conversationId, GuidHeader(produced, TransportHeaders.ConversationId));
        Assert.Equal("orders", Header(produced, TransportHeaders.ConversationAddress));
        Assert.Equal("orders", Header(produced, TransportHeaders.MessageOriginAddress));
        Assert.Equal("payments", Header(produced, TransportHeaders.MessageDestinationAddress));
        Assert.Equal("0", Header(produced, TransportHeaders.RetryCount));
        Assert.NotEqual(inboundMessageId, GuidHeader(produced, TransportHeaders.MessageId));
        Assert.Equal(typeof(TestEvent).FullName, Header(produced, TransportHeaders.MessageType));
        Assert.Equal(message.AggregateId, GuidHeader(produced, TransportHeaders.AggregateId));
        Assert.Equal("g1", Header(produced, TransportHeaders.AggregateConsumers));
    }

    [Fact]
    public async Task Send_ProduceFails_Rethrows()
    {
        _producer.Failure = new ProduceException<Null, byte[]>(new Confluent.Kafka.Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        await Assert.ThrowsAsync<ProduceException<Null, byte[]>>(
            () => CreateSut((typeof(TestCommand), "orders")).Send(new TestCommand("pepe"), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Send_SerializesTheRuntimeType()
    {
        await CreateSut((typeof(TestCommand), "orders")).Send(new TestCommand("pepe"), TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Message.Value);

        Assert.Equal("pepe", body.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Publish_Batch_ProducesEveryEventInOrder()
    {
        TestEvent[] events = [new("a"), new("b"), new("c")];

        await CreateSut((typeof(TestEvent), "orders.created")).Publish(events, TestContext.Current.CancellationToken);

        Assert.Equal(3, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced => Assert.Equal("orders.created", produced.Topic));
        Assert.Equal(["a", "b", "c"], _producer.Produced.Select(produced => JsonSerializer.Deserialize<JsonElement>(produced.Message.Value).GetProperty("Name").GetString()));
    }

    [Fact]
    public async Task Send_Batch_ProducesEveryCommand()
    {
        TestCommand[] commands = [new("x"), new("y")];

        await CreateSut((typeof(TestCommand), "orders")).Send(commands, TestContext.Current.CancellationToken);

        Assert.Equal(2, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced => Assert.Equal("orders", produced.Topic));
    }

    [Fact]
    public async Task Send_BatchWithTransport_ContinuesTheConversationForEach()
    {
        Guid conversationId = Guid.NewGuid();

        Headers inbound = new()
        {
            { TransportHeaders.MessageId, Guid.NewGuid().ToByteArray() },
            { TransportHeaders.MessageDestinationAddress, "orders"u8.ToArray() },
            { TransportHeaders.ConversationId, conversationId.ToByteArray() },
            { TransportHeaders.ConversationAddress, "orders"u8.ToArray() },
            { TransportHeaders.ConversationOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
            { TransportHeaders.AggregateId, Guid.NewGuid().ToByteArray() },
            { TransportHeaders.AggregateCorrelationId, Guid.NewGuid().ToByteArray() },
            { TransportHeaders.AggregateOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) },
            { TransportHeaders.AggregateConsumers, "old"u8.ToArray() },
            { TransportHeaders.RetryCount, "3"u8.ToArray() }
        };

        Transport transport = new(inbound.ToImmutableList(), "orders", new Partition(0), new Offset(10), null, new Timestamp(DateTime.UtcNow));
        TestCommand[] commands = [new("x"), new("y")];

        await CreateSut((typeof(TestCommand), "payments")).Send(commands, transport, TestContext.Current.CancellationToken);

        Assert.Equal(2, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced =>
        {
            Assert.Equal("payments", produced.Topic);
            Assert.Equal(conversationId, GuidHeader(produced.Message, TransportHeaders.ConversationId));
            Assert.Equal("orders", Header(produced.Message, TransportHeaders.MessageOriginAddress));
            Assert.Equal("payments", Header(produced.Message, TransportHeaders.MessageDestinationAddress));
            Assert.Equal("0", Header(produced.Message, TransportHeaders.RetryCount));
        });

        Assert.Equal(2, _producer.Produced.Select(produced => GuidHeader(produced.Message, TransportHeaders.MessageId)).Distinct().Count());
    }

    [Fact]
    public async Task Publish_EmptyBatch_ProducesNothing()
    {
        await CreateSut((typeof(TestEvent), "orders.created")).Publish(Array.Empty<TestEvent>(), TestContext.Current.CancellationToken);

        Assert.Empty(_producer.Produced);
    }
}
