using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// End-to-end proof of the event worker's fanout over a real broker: one published event reaches two
/// subscribers on two separate consumer groups reading the same topic. Exercises the whole publish →
/// two consumer groups path over the container — the first real coverage of the event worker's fanout
/// (the roundtrip suite covers only a command's point-to-point topic).
/// <para>
/// Kafka caveat vs the RabbitMQ mirror: fanout is N consumer groups on one topic, not a fanout exchange
/// with N bound queues. With the event's <c>AggregateConsumers</c> empty, neither group filters it out,
/// so both process it.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class FanoutTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "fanout-integration-tests";
    private const string FirstGroupId = "fanout-integration-tests.first";
    private const string SecondGroupId = "fanout-integration-tests.second";

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public FanoutTests(KafkaFixture fixture)
        => _fixture = fixture;

    /// <summary>One published event is delivered to two subscribers on two consumer groups reading its topic.</summary>
    [Fact]
    public async Task Publish_fansTheEventOutToEverySubscriberGroup_BothReceiveIt()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        FanoutProbe probe = new FanoutProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddEvent<FanoutEvent>(Topic),
            consumer => consumer
                .AddEventSubscriber<FanoutEvent, FirstFanoutSubscriber>(FirstGroupId)
                .AddEventSubscriber<FanoutEvent, SecondFanoutSubscriber>(SecondGroupId));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            const string payload = "hello-fanout";

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Publish(new FanoutEvent(payload), cancellationToken);
            }

            Task both = Task.WhenAll(probe.First, probe.Second);
            Task completed = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(120), cancellationToken));
            Assert.True(completed == both, "The event did not reach both subscriber groups within 120 seconds.");

            Assert.Equal(payload, await probe.First);
            Assert.Equal(payload, await probe.Second);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
