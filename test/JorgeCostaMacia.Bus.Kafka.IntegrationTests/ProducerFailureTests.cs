using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// Settles the open question of how the real Confluent client signals a failed produce — the analog of
/// the RabbitMQ mandatory-return test. The bus's <c>Producer</c> and both error handlers classify a
/// produce failure by catching <see cref="ProduceException{TKey, TValue}"/> over <see cref="Null"/> /
/// <c>byte[]</c> (with a <c>Local_QueueFull</c> special case); this drives a real produce failure
/// against the live broker and captures the actual exception type and <see cref="ErrorCode"/>, then
/// verifies the catch-blocks would classify it correctly.
/// <para>
/// The trigger is a broker rejection: a topic capped to a tiny <c>max.message.bytes</c> and an oversized
/// message produced to it (the client sends it, the broker rejects it) — the same shape as any produce
/// the bus cannot complete. Discovered against Confluent.Kafka 2.15.0 + cp-kafka 7.5.12: it surfaces as
/// <see cref="ProduceException{Null, Byte}"/> with <see cref="Error.Code"/> ==
/// <see cref="ErrorCode.MsgSizeTooLarge"/>. Because it IS a
/// <see cref="ProduceException{Null, Byte}"/>, the bus's <c>catch (ProduceException&lt;Null, byte[]&gt;)</c>
/// lane catches it — the classification is correct — and this test guards that a future client that
/// reshaped the type would fail here.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class ProducerFailureTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "producer-failure-integration-tests";
    private const int MaxMessageBytes = 1_000;

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public ProducerFailureTests(KafkaFixture fixture)
        => _fixture = fixture;

    /// <summary>An oversized produce to a size-capped topic throws <see cref="ProduceException{Null, Byte}"/> with <see cref="ErrorCode.MsgSizeTooLarge"/> — the type the bus's produce catch-blocks depend on.</summary>
    [Fact]
    public async Task Produce_aMessageLargerThanTheTopicAllows_ThrowsAProduceExceptionClassifiedByErrorCode()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        await Broker.CreateCappedTopicAsync(configuration, Topic, MaxMessageBytes, cancellationToken);

        using IProducer<Null, byte[]> producer = Broker.Producer(configuration);

        // Well above the topic's tiny cap (and within the client's own message.max.bytes, so the client
        // sends it and the broker is the one that rejects) — a genuine broker-side produce failure.
        byte[] oversized = Encoding.UTF8.GetBytes(new string('x', 64 * 1024));

        Exception caught = await Assert.ThrowsAnyAsync<Exception>(
            () => producer.ProduceAsync(Topic, new Message<Null, byte[]> { Value = oversized }, cancellationToken));

        // The discovered type: the bus's Producer wraps an IProducer<Null, byte[]> and every catch is
        // over exactly this closed type — so a failed produce must surface as it.
        ProduceException<Null, byte[]> produce = Assert.IsType<ProduceException<Null, byte[]>>(caught);

        // The discovered code: a broker rejection of an oversized message.
        Assert.Equal(ErrorCode.MsgSizeTooLarge, produce.Error.Code);

        // The classification the bus depends on: it is NOT Local_QueueFull, so the Producer routes it to
        // its generic catch (ProduceException<Null, byte[]>) → SendFaulted → rethrow, and the error
        // handlers to their catch (ProduceException<Null, byte[]>) → Unhandled lane. Never the
        // catch (Exception) branch.
        Assert.NotEqual(ErrorCode.Local_QueueFull, produce.Error.Code);
    }
}
