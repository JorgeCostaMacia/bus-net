using System.Text;
using Confluent.Kafka;
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
}
