using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class CommandWorkerTests
{
    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();
    private readonly LifetimeFake _lifetime = new();
    private readonly RecordingCommandHandler _handler = new();

    private CommandWorker<TestCommand, RecordingCommandHandler> Worker(ConsumerFake consumer, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_handler)
            .AddScoped<Domain.Commands.Errors.CommandErrorHandler<TestCommand, RecordingCommandHandler>>(_ =>
                new CommandErrorHandler<TestCommand, RecordingCommandHandler>(_producer, _scheduler, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID, intervals ?? [], []))
            .AddScoped<Domain.Commands.Faults.CommandFaultHandler<TestCommand, RecordingCommandHandler>>(_ =>
                new CommandFaultHandler<TestCommand, RecordingCommandHandler>(_producer, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID))
            .BuildServiceProvider();

        return new CommandWorker<TestCommand, RecordingCommandHandler>(
            consumer,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<CommandWorker<TestCommand, RecordingCommandHandler>>.Instance,
            _lifetime,
            Deliveries.TOPIC,
            Deliveries.GROUP_ID);
    }

    private async Task Drive(CommandWorker<TestCommand, RecordingCommandHandler> worker, ConsumerFake consumer)
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);
        await consumer.Drained.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task HandlerSucceeds_StoresTheOffset_AndProducesNothing()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(Deliveries.TOPIC, consumer.SubscribedTopic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
    }

    [Fact]
    public async Task HandlerThrows_NoLadder_ParksToErrorTopic_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.error", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task HandlerThrows_ZeroInterval_RequeuesToTopicTail_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer, [TimeSpan.Zero]), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.TOPIC, topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MalformedBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.Garbage());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task NullBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.NullBody());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task Tombstone_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.Tombstone());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MultipleDeliveries_ProcessedInOrder_EachStored()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("a"), 10), Deliveries.Delivery(new TestCommand("b"), 11));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("b", _handler.Received?.Name);
        Assert.Equal([10L, 11L], consumer.Stored.Select(offset => offset.Offset.Value));
    }

    [Fact]
    public async Task IgnoresAggregateConsumers_AlwaysProcesses()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe"), consumers: "other.handler"));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }
}
