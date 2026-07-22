using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// End-to-end proof of the error lane: a command whose handler always throws, with a retry ladder that
/// exhausts, is parked to <c>{topic}.error</c> — the terminal park. The assertion reads the message
/// back straight from the broker (a bare consumer on a throwaway group at
/// <see cref="AutoOffsetReset.Earliest"/>, bypassing the bus), so it observes the parked error truly
/// sitting on the lazily-born <c>.error</c> topic, stamped with the failing group as its error group.
/// Exercises <c>CommandErrorHandler.ParkError</c> end to end.
/// <para>
/// Kafka caveat vs the RabbitMQ mirror: the park lane is a TOPIC, not a queue, so the readback joins a
/// fresh consumer group at <see cref="AutoOffsetReset.Earliest"/> and reads it from offset zero.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class ErrorParkingTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "error-parking-integration-tests";
    private const string GroupId = "error-parking-integration-tests.handler";
    private const string ErrorTopic = Topic + ".error";

    // The wire contract of the parked error's headers (the bus's internal TransportHeaders, read here as literals).
    private const string ErrorGroupIdHeader = "jcm-error-group-id";
    private const string ErrorTypeHeader = "jcm-error-type";

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public ErrorParkingTests(KafkaFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A handler that always throws exhausts its retry ladder and the failure is parked to the topic's <c>.error</c>.</summary>
    [Fact]
    public async Task Send_toAHandlerThatAlwaysThrowsWithAnExhaustingLadder_ParksTheFailureToTheErrorTopic()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<ParkingCommand>(Topic),
            // A single 00:00 rung: the original fails and is re-produced immediately, the redelivery
            // fails again, the budget is spent — so the failure parks to .error as terminal.
            consumer => consumer.AddCommandHandler<ParkingCommand, ParkingCommandHandler>(GroupId, retryIntervals: ImmutableList.Create(TimeSpan.Zero)));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new ParkingCommand("hello-error-park"), cancellationToken);
            }

            ConsumeResult<Ignore, byte[]>? parked = await Broker.WaitForParkedAsync(configuration, ErrorTopic, TimeSpan.FromSeconds(120), cancellationToken);

            Assert.True(parked is not null, $"Nothing was parked to '{ErrorTopic}' within 120 seconds.");
            Assert.Equal(GroupId, Broker.Header(parked, ErrorGroupIdHeader));
            Assert.Equal(typeof(InvalidOperationException).FullName, Broker.Header(parked, ErrorTypeHeader));
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
