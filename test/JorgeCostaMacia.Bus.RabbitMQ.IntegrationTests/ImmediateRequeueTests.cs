using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end proof of the immediate-requeue retry path (distinct from the Quartz scheduled retry): a
/// <c>00:00</c> retry step re-publishes a failed command to its exchange for an instant redelivery. The
/// handler throws on the first delivery and succeeds on the redelivery, so the test proves the handler
/// ran exactly twice and the message was eventually acked — exactly-once eventual success, no hot spin.
/// </summary>
[Trait("Category", "Integration")]
public sealed class ImmediateRequeueTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "immediate-requeue-integration-tests";
    private const string Queue = "immediate-requeue-integration-tests.handler";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public ImmediateRequeueTests(RabbitMqFixture fixture)
        => _fixture = fixture;

    /// <summary>A handler that fails once, with a <c>00:00</c> retry step, is redelivered immediately and succeeds — invoked exactly twice, then acked.</summary>
    [Fact]
    public async Task Send_ToAHandlerThatFailsOnce_IsRedeliveredAndSucceeds()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RequeueProbe probe = new();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<RequeueCommand>(Exchange),
            // A single 00:00 rung: the first failure re-publishes to the exchange immediately for an
            // instant redelivery — the immediate-requeue path, no scheduler involved.
            consumer => consumer.AddCommandHandler<RequeueCommand, RequeueCommandHandler>(Queue, retryIntervals: [TimeSpan.Zero]));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new RequeueCommand("hello-immediate-requeue"), cancellationToken);
            }

            Task completed = await Task.WhenAny(probe.Succeeded, Task.Delay(TimeSpan.FromSeconds(30), cancellationToken));
            Assert.True(completed == probe.Succeeded, "The immediate retry did not succeed within 30 seconds.");

            // Settle: give any spurious third delivery a window to show up, then assert exactly-once
            // eventual success — the failing original plus the one successful redelivery, then acked.
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.Equal(2, probe.Invocations);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
