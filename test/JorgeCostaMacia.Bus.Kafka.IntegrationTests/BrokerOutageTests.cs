using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// Chaos test for a broker outage mid-load: the whole batch is produced, and while the consumer is
/// draining it the broker is frozen (Docker pause) for a few seconds and then thawed. The bus must
/// recover on its own and every distinct record must still be handled — at-least-once with <b>no loss</b>
/// across the outage. Any redeliveries the outage causes surface as the gap between total and distinct
/// deliveries (reported, not asserted — they are expected and absorbed by dedup on the consuming side).
/// </summary>
[Trait("Category", "Integration")]
public sealed class BrokerOutageTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "broker-outage-integration-tests";
    private const string GroupId = "broker-outage-integration-tests.handler";
    private const int Records = 100;
    private const int PauseAfterUnique = 10;
    private static readonly TimeSpan Outage = TimeSpan.FromSeconds(4);

    private readonly KafkaFixture _fixture;
    private readonly ITestOutputHelper _output;

    /// <summary>Takes the shared broker fixture and the test output sink.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    /// <param name="output">The sink the recovery evidence is written to.</param>
    public BrokerOutageTests(KafkaFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>Every distinct record is handled even though the broker is frozen mid-drain — no record is lost across the outage.</summary>
    [Fact]
    public async Task Send_aBatchAndFreezeTheBrokerMidDrain_RecoversAndHandlesEveryDistinctRecord()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();
        RecoveryProbe probe = new RecoveryProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<ChaosCommand>(Topic),
            consumer => consumer.AddCommandHandler<ChaosCommand, RecoveryCommandHandler>(GroupId));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            probe.Expect(Records);

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();

                for (int index = 0; index < Records; index++)
                {
                    await bus.Send(new ChaosCommand($"record-{index}"), cancellationToken);
                }
            }

            // Freeze the broker once the drain is underway, then thaw it — a real outage mid-load.
            Task outage = InjectOutageMidDrainAsync(probe, cancellationToken);

            Task completed = await Task.WhenAny(probe.AllUniqueHandled, Task.Delay(TimeSpan.FromSeconds(180), cancellationToken));

            // Ensure the pause/unpause finished so a frozen broker never leaks into the next test.
            await outage;

            Assert.True(
                completed == probe.AllUniqueHandled,
                $"Only {probe.UniqueHandled} of {Records} distinct records survived the broker outage — a record was lost across the outage.");

            _output.WriteLine($"Kafka broker outage: {probe.UniqueHandled}/{Records} distinct handled, {probe.TotalHandled} total deliveries ({probe.TotalHandled - probe.UniqueHandled} at-least-once redeliveries across the outage).");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>Waits for the drain to get underway, freezes the broker for <see cref="Outage"/>, then thaws it.</summary>
    /// <param name="probe">The probe whose progress marks the drain starting.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    private async Task InjectOutageMidDrainAsync(RecoveryProbe probe, CancellationToken cancellationToken)
    {
        await WaitUntilAsync(() => probe.UniqueHandled >= PauseAfterUnique, TimeSpan.FromSeconds(90), cancellationToken);

        await _fixture.PauseAsync(cancellationToken);
        await Task.Delay(Outage, cancellationToken);
        await _fixture.UnpauseAsync(cancellationToken);
    }

    /// <summary>Polls <paramref name="condition"/> until it holds or the timeout elapses.</summary>
    /// <param name="condition">The condition awaited.</param>
    /// <param name="timeout">The longest time to wait.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("The drain did not get underway before the outage window.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        }
    }
}
