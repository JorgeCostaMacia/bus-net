using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using RabbitProducer = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers.Producer;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

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

    private static string? Header(IReadOnlyDictionary<string, object?> headers, string key)
        => headers.TryGetValue(key, out object? value) && value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : null;
}
