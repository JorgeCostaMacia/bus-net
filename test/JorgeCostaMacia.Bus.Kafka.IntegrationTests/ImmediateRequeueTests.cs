using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// End-to-end proof of the immediate-requeue retry path (distinct from the Quartz scheduled retry): a
/// <c>00:00</c> retry step re-produces a failed message to its topic for an instant redelivery. The
/// handler throws on the first delivery and succeeds on the redelivery, so the test proves the handler
/// ran exactly twice — exactly-once eventual success, no hot spin.
/// <para>
/// Kafka caveat vs the RabbitMQ mirror: a requeue is a produce back to the topic tail at a NEW offset,
/// not an in-place redelivery. The event variant additionally proves the retry is re-targeted to its
/// own group via the <c>AggregateConsumers</c> header — a Kafka-specific behavior previously covered
/// only by unit tests.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class ImmediateRequeueTests : IClassFixture<KafkaFixture>
{
    private const string CommandTopic = "immediate-requeue-integration-tests";
    private const string CommandGroupId = "immediate-requeue-integration-tests.handler";

    private const string EventTopic = "immediate-requeue-event-integration-tests";
    private const string FailingGroupId = "immediate-requeue-event-integration-tests.failing";
    private const string OtherGroupId = "immediate-requeue-event-integration-tests.other";

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public ImmediateRequeueTests(KafkaFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A command handler that fails once, with a <c>00:00</c> retry step, is redelivered immediately and succeeds — invoked exactly twice.</summary>
    [Fact]
    public async Task Send_toAHandlerThatFailsOnceWithAnImmediateRetryStep_IsRedeliveredAndSucceedsExactlyTwice()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RequeueProbe probe = new RequeueProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<RequeueCommand>(CommandTopic),
            // A single 00:00 rung: the first failure re-produces to the topic immediately for an
            // instant redelivery — the immediate-requeue path, no scheduler involved.
            consumer => consumer.AddCommandHandler<RequeueCommand, RequeueCommandHandler>(CommandGroupId, retryIntervals: ImmutableList.Create(TimeSpan.Zero)));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new RequeueCommand("hello-immediate-requeue"), cancellationToken);
            }

            Task completed = await Task.WhenAny(probe.Succeeded, Task.Delay(TimeSpan.FromSeconds(120), cancellationToken));
            Assert.True(completed == probe.Succeeded, "The immediate retry did not succeed within 120 seconds.");

            // Settle: give any spurious third delivery a window to show up, then assert exactly-once
            // eventual success — the failing original plus the one successful redelivery.
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

            Assert.Equal(2, probe.Invocations);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>An event subscriber that fails once has its immediate retry re-targeted to its own group only — the failing group runs twice, the other group (which handled the original) runs exactly once.</summary>
    [Fact]
    public async Task Publish_toTwoGroupsWhereOneFailsOnce_ReTargetsTheImmediateRetryToTheFailingGroupOnly()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RetargetProbe probe = new RetargetProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddEvent<RequeueEvent>(EventTopic),
            consumer => consumer
                // The failing group carries a 00:00 rung, so its first failure re-produces the event
                // immediately, re-targeted (via AggregateConsumers) to this group only.
                .AddEventSubscriber<RequeueEvent, RetargetFailingSubscriber>(FailingGroupId, retryIntervals: ImmutableList.Create(TimeSpan.Zero))
                .AddEventSubscriber<RequeueEvent, RetargetOtherSubscriber>(OtherGroupId));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Publish(new RequeueEvent("hello-retarget"), cancellationToken);
            }

            // Both signals must land: the failing group's re-targeted retry succeeded, and the other
            // group received the original — so a still-zero "other" count would be a real miss, not a race.
            Task both = Task.WhenAll(probe.FailingSucceeded, probe.OtherReceived);
            Task completed = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(120), cancellationToken));
            Assert.True(completed == both, "The failing group's retry and the other group's original did not both arrive within 120 seconds.");

            // Settle: give the re-targeted retry a window to (wrongly) reach the other group, proving
            // the AggregateConsumers filter kept it out.
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

            Assert.Equal(2, probe.FailingInvocations);
            Assert.Equal(1, probe.OtherInvocations);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
