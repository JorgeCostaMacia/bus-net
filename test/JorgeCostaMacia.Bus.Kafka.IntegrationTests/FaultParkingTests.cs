using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// End-to-end proof of the fault lane: a malformed body — raw bytes that never deserialize to the
/// command — is produced straight to the consumer's topic, so the delivery breaks while building the
/// context (before any handler runs) and is parked to <c>{topic}.fault</c>. The assertion reads the
/// message back straight from the broker (a bare consumer on a throwaway group at
/// <see cref="AutoOffsetReset.Earliest"/>, bypassing the bus), so it observes the parked fault truly
/// sitting on the lazily-born <c>.fault</c> topic, stamped with the failing group.
/// </summary>
[Trait("Category", "Integration")]
public sealed class FaultParkingTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "fault-parking-integration-tests";
    private const string GroupId = "fault-parking-integration-tests.handler";
    private const string FaultTopic = Topic + ".fault";

    // The wire contract of the parked fault's headers (the bus's internal TransportHeaders, read here as a literal).
    private const string ErrorGroupIdHeader = "jcm-error-group-id";

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public FaultParkingTests(KafkaFixture fixture)
        => _fixture = fixture;

    /// <summary>A malformed body that cannot be deserialized breaks the delivery and is parked to the topic's <c>.fault</c>.</summary>
    [Fact]
    public async Task Deliver_aMalformedBodyThatCannotBeDeserialized_ParksTheDeliveryToTheFaultTopic()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<FaultCommand>(Topic),
            consumer => consumer.AddCommandHandler<FaultCommand, FaultCommandHandler>(GroupId));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            // Produce a body that will never deserialize to the command, straight to the topic the
            // consumer subscribes to — the worker receives it (Earliest, so a produce before the group
            // joined still reads) and breaks in CreateContext before the handler.
            using (IProducer<Null, byte[]> producer = Broker.Producer(configuration))
            {
                byte[] malformed = Encoding.UTF8.GetBytes("this-is-not-the-json-of-a-command");

                await producer.ProduceAsync(Topic, new Message<Null, byte[]> { Value = malformed }, cancellationToken);
                producer.Flush(cancellationToken);
            }

            ConsumeResult<Ignore, byte[]>? parked = await Broker.WaitForParkedAsync(configuration, FaultTopic, TimeSpan.FromSeconds(120), cancellationToken);

            Assert.True(parked is not null, $"Nothing was parked to '{FaultTopic}' within 120 seconds.");
            Assert.Equal(GroupId, Broker.Header(parked, ErrorGroupIdHeader));
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
