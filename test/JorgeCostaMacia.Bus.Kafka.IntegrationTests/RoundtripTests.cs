using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// End-to-end roundtrip against a real Kafka broker: connects the producer, produces a command to its
/// topic, and asserts the hosted consumer deserializes it and invokes the handler with the original
/// payload. Exercises the whole produce → consume path over the container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RoundtripTests : IClassFixture<KafkaFixture>
{
    private const string Topic = "integration-tests";
    private const string GroupId = "integration-tests.handler";

    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public RoundtripTests(KafkaFixture fixture)
        => _fixture = fixture;

    /// <summary>A sent command is delivered to its handler carrying the original payload.</summary>
    [Fact]
    public async Task Send_delivers_the_command_to_its_handler_with_its_payload()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        TaskCompletionSource<IntegrationCommand> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<IntegrationCommand>(Topic),
            consumer => consumer.AddCommandHandler<IntegrationCommand, IntegrationCommandHandler>(GroupId));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            const string payload = "hello-roundtrip";

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new IntegrationCommand(payload), cancellationToken);
            }

            // Kafka needs longer than RabbitMQ here: the topic auto-creates on first produce and the
            // consumer group must join and be assigned the partition before it fetches. AutoOffsetReset
            // defaults to Earliest, so a consumer that joins after the produce still reads the message.
            Task completed = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(60), cancellationToken));
            Assert.True(completed == received.Task, "The command was not delivered to its handler within 60 seconds.");

            IntegrationCommand delivered = await received.Task;
            Assert.Equal(payload, delivered.Payload);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
