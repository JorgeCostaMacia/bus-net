using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Events;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class EventWorkerTests
{
    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();
    private readonly LifetimeFake _lifetime = new();
    private readonly RecordingEventSubscriber _subscriber = new();

    private EventWorker<TestEvent, RecordingEventSubscriber> Worker(ConsumerFake consumer, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_subscriber)
            .AddScoped<Domain.Events.Errors.EventErrorHandler<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventErrorHandler<TestEvent, RecordingEventSubscriber>(_producer, _scheduler, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID, intervals ?? [], []))
            .AddScoped<Domain.Events.Faults.EventFaultHandler<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventFaultHandler<TestEvent, RecordingEventSubscriber>(_producer, NullLogger.Instance, Deliveries.TOPIC, Deliveries.GROUP_ID))
            .BuildServiceProvider();

        return new EventWorker<TestEvent, RecordingEventSubscriber>(
            consumer,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<EventWorker<TestEvent, RecordingEventSubscriber>>.Instance,
            _lifetime,
            Deliveries.TOPIC,
            Deliveries.GROUP_ID);
    }

    private async Task Drive(EventWorker<TestEvent, RecordingEventSubscriber> worker, ConsumerFake consumer)
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);
        await consumer.Drained.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SubscriberSucceeds_StoresTheOffset_AndProducesNothing()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe")));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task TargetsOtherConsumers_SkipsAndStores_WithoutRunningTheSubscriber()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe"), consumers: "billing.on.orders.subscriber"));

        await Drive(Worker(consumer), consumer);

        Assert.Null(_subscriber.Received);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task TargetsItsGroup_Runs()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe"), consumers: $"other.subscriber,{Deliveries.GROUP_ID}"));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task SubscriberThrows_NoLadder_ParksToErrorTopic_AndStores()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe")));

        await Drive(Worker(consumer), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.error", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task SubscriberThrows_ZeroInterval_RequeuesToTopicTail_AndStores()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe")));

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

        Assert.Null(_subscriber.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task NullBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.NullBody());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_subscriber.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MultipleDeliveries_ProcessedInOrder_EachStored()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("a"), 10), Deliveries.Delivery(new TestEvent("b"), 11));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("b", _subscriber.Received?.Name);
        Assert.Equal([10L, 11L], consumer.Stored.Select(offset => offset.Offset.Value));
    }
}
