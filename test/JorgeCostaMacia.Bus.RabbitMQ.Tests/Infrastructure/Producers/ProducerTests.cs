using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using RabbitProducer = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers.Producer;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Producers;

public class ProducerTests
{
    private readonly ChannelFake _channel = new();
    private readonly ConnectionFake _connection;

    public ProducerTests() => _connection = new ConnectionFake(_channel);

    private RabbitProducer Sut() => new(_connection);

    private static Dictionary<string, object?> Headers() => [];

    [Fact]
    public async Task Produce_PublishesToTheExchangeWithTheRoutingKey()
    {
        await Sut().Produce("orders", "rk", "hi"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal("orders", publish.Exchange);
        Assert.Equal("rk", publish.RoutingKey);
        Assert.Equal("hi", Encoding.UTF8.GetString(publish.Body.Span));
    }

    [Fact]
    public async Task Produce_MarksTheMessagePersistent()
    {
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.True(Assert.Single(_channel.Published).Persistent);
    }

    [Fact]
    public async Task Produce_SameExchangeTwice_ReusesTheDestinationChannel()
    {
        // one long-lived channel per destination: the second publish to the same exchange rides the
        // cached channel instead of opening another.
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(1, _connection.Created);
    }

    [Fact]
    public async Task Produce_DifferentExchanges_OpenOneChannelEach()
    {
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.Produce("orders.created", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(2, _connection.Created);
    }

    [Fact]
    public async Task Produce_ChannelDied_ReopensInsteadOfReusingTheDeadOne()
    {
        // an async publish error closes the channel broker-side: the next produce to that exchange
        // must replace it instead of handing out the dead cached one for the application's lifetime.
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(1, _connection.Created);

        _channel.IsOpen = false;
        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(2, _connection.Created);
    }

    [Fact]
    public async Task DisposeAsync_ClosesEveryDestinationChannel()
    {
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.DisposeAsync();

        Assert.True(_channel.Disposed);
    }

    [Fact]
    public async Task Park_RedeclaresTheDurableQueue_AndPublishesMandatoryThroughTheDefaultExchange()
    {
        // the loss-proof park: the idempotent declare recreates a park queue deleted at runtime
        // (the consumers' exact options), and mandatory makes an unroutable park throw instead of
        // being dropped — and confirmed — silently.
        await Sut().Park("orders.handler.error", "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        (string queue, bool durable, bool exclusive, bool autoDelete) = Assert.Single(_channel.QueuesDeclared);
        Assert.Equal("orders.handler.error", queue);
        Assert.True(durable);
        Assert.False(exclusive);
        Assert.False(autoDelete);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal(string.Empty, publish.Exchange);
        Assert.Equal("orders.handler.error", publish.RoutingKey);
        Assert.True(publish.Mandatory);
    }

    [Fact]
    public async Task Produce_PublishesWithoutMandatory()
    {
        // an unroutable normal publish is legitimate (a fanout with no subscribers) — only parks trip.
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.False(Assert.Single(_channel.Published).Mandatory);
    }

    [Fact]
    public async Task Produce_Failure_Rethrows()
    {
        _channel.PublishFailure = new InvalidOperationException("broker down");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Produce_StampsTheHostHeaders()
    {
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, object?> headers = Assert.Single(_channel.Published).Headers!;
        Assert.Equal(Environment.MachineName, Header(headers, TransportHeaders.HostMachineName));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostAssembly)));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostBusVersion)));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostFrameworkVersion)));
    }

    [Fact]
    public async Task Produce_ReStampsTheHost_OverGivenHeaders()
    {
        Dictionary<string, object?> headers = new()
        {
            [TransportHeaders.HostMachineName] = "another-host"u8.ToArray()
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        Assert.Equal(Environment.MachineName, Header(Assert.Single(_channel.Published).Headers!, TransportHeaders.HostMachineName));
    }

    [Fact]
    public async Task Produce_CarriesTheGivenEnvelopeHeaders()
    {
        Guid messageId = Guid.NewGuid();
        Dictionary<string, object?> headers = new()
        {
            [TransportHeaders.MessageId] = messageId.ToByteArray()
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        Assert.Equal(messageId, new Guid((byte[])Assert.Single(_channel.Published).Headers![TransportHeaders.MessageId]!));
    }

    [Fact]
    public async Task Produce_MapsTheEnvelopeToNativeAmqpProperties()
    {
        Guid messageId = Guid.NewGuid();
        Guid conversationId = Guid.NewGuid();
        DateTime occurredAt = new(2026, 7, 7, 12, 30, 45, DateTimeKind.Utc);

        Dictionary<string, object?> headers = new()
        {
            [TransportHeaders.MessageId] = messageId.ToByteArray(),
            [TransportHeaders.ConversationId] = conversationId.ToByteArray(),
            [TransportHeaders.MessageType] = "Orders.PlaceOrder"u8.ToArray(),
            [TransportHeaders.MessageOccurredAt] = Encoding.UTF8.GetBytes(occurredAt.ToString("O"))
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal(messageId.ToString(), publish.MessageId);
        Assert.Equal(conversationId.ToString(), publish.CorrelationId);
        Assert.Equal("Orders.PlaceOrder", publish.Type);
        Assert.Equal("application/json", publish.ContentType);
        Assert.Equal(new DateTimeOffset(occurredAt).ToUnixTimeSeconds(), publish.Timestamp);
        Assert.False(string.IsNullOrWhiteSpace(publish.AppId)); // the host assembly, stamped by the producer
    }

    private static string? Header(IReadOnlyDictionary<string, object?> headers, string key)
        => headers.TryGetValue(key, out object? value) && value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : null;
}
