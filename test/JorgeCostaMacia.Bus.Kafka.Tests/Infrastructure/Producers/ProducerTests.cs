using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using KafkaProducer = JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers.Producer;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ProducerTests
{
    private readonly KafkaProducerFake _kafka = new();

    private KafkaProducer Sut() => new(_kafka, NullLogger<KafkaProducer>.Instance);

    private static Message<Null, byte[]> Message(string value = "{}") => new() { Value = Encoding.UTF8.GetBytes(value) };

    [Fact]
    public async Task Produce_Success_ForwardsToTheClient()
    {
        await Sut().Produce("orders", Message("hi"), TestContext.Current.CancellationToken);

        (string topic, _) = Assert.Single(_kafka.Produced);
        Assert.Equal("orders", topic);
    }

    [Fact]
    public async Task Produce_QueueFull_Rethrows()
    {
        _kafka.ProduceFailure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_QueueFull), new DeliveryResult<Null, byte[]>());

        await Assert.ThrowsAsync<ProduceException<Null, byte[]>>(() => Sut().Produce("orders", Message(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Produce_GenericProduceException_Rethrows()
    {
        _kafka.ProduceFailure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());

        await Assert.ThrowsAsync<ProduceException<Null, byte[]>>(() => Sut().Produce("orders", Message(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Produce_StampsTheHostHeaders()
    {
        await Sut().Produce("orders", Message(), TestContext.Current.CancellationToken);

        Message<Null, byte[]> produced = Assert.Single(_kafka.Produced).Message;
        Assert.Equal(Environment.MachineName, Deliveries.Header(produced, TransportHeaders.HostMachineName));
        Assert.False(string.IsNullOrWhiteSpace(Deliveries.Header(produced, TransportHeaders.HostAssembly)));
        Assert.False(string.IsNullOrWhiteSpace(Deliveries.Header(produced, TransportHeaders.HostBusVersion)));
    }

    [Fact]
    public async Task Produce_ReStampsTheHost_OverAClonedEnvelope()
    {
        Message<Null, byte[]> message = Message();
        message.Headers = [new Header(TransportHeaders.HostMachineName, "another-host"u8.ToArray())];

        await Sut().Produce("orders", message, TestContext.Current.CancellationToken);

        Message<Null, byte[]> produced = Assert.Single(_kafka.Produced).Message;
        Assert.Equal(Environment.MachineName, Deliveries.Header(produced, TransportHeaders.HostMachineName));
        Assert.Single(produced.Headers, header => header.Key == TransportHeaders.HostMachineName);
    }

    [Fact]
    public async Task Produce_Batch_ProducesEveryPairInOrder()
    {
        List<KeyValuePair<string, Message<Null, byte[]>>> messages =
        [
            new("orders", Message("a")),
            new("payments", Message("b"))
        ];

        await Sut().Produce(messages, TestContext.Current.CancellationToken);

        Assert.Equal(["orders", "payments"], _kafka.Produced.Select(produced => produced.Topic));
    }

    [Fact]
    public async Task Produce_Batch_OneFails_ThrowsTheFailure_AndStillProducesTheRest()
    {
        // the pairs are enqueued together and awaited together: one failing pair faults the await
        // (awaiting still means broker-acked for EVERY message), while the other pairs are still
        // produced — a partial batch failure does not roll back nor stop the rest.
        _kafka.ProduceFailure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<Null, byte[]>());
        _kafka.FailingTopics.Add("payments");
        List<KeyValuePair<string, Message<Null, byte[]>>> messages =
        [
            new("orders", Message("a")),
            new("payments", Message("b")),
            new("shipping", Message("c"))
        ];

        await Assert.ThrowsAsync<ProduceException<Null, byte[]>>(() => Sut().Produce(messages, TestContext.Current.CancellationToken));

        Assert.Equal(["orders", "shipping"], _kafka.Produced.Select(produced => produced.Topic));
    }
}
