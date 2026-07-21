using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Faults;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Events;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Startup;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers.Events;

public class EventWorkerTests
{
    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RetrySchedulerFake _scheduler = new RetrySchedulerFake();
    private readonly LifetimeFake _lifetime = new LifetimeFake();
    private readonly BusHealth _health = new BusHealth();
    private readonly RecordingEventSubscriber _subscriber = new RecordingEventSubscriber();

    private EventWorker<TestEvent, RecordingEventSubscriber> Worker(ConsumerFake consumer, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_subscriber)
            .AddScoped<EventErrorHandlerBase<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventErrorHandler<TestEvent, RecordingEventSubscriber>(_producer, _scheduler, NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId, intervals ?? ImmutableList<TimeSpan>.Empty, ImmutableList<Type>.Empty))
            .AddScoped<EventFaultHandlerBase<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventFaultHandler<TestEvent, RecordingEventSubscriber>(_producer, NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId))
            .BuildServiceProvider();

        return new EventWorker<TestEvent, RecordingEventSubscriber>(
            consumer,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<EventWorker<TestEvent, RecordingEventSubscriber>>.Instance,
            _lifetime,
            _health,
            new StartupGate(8),
            new StartupSignal(),
            Deliveries.Topic,
            Deliveries.GroupId);
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
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe"), consumers: $"other.subscriber,{Deliveries.GroupId}"));

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
        Assert.Equal($"{Deliveries.Topic}.error", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task SubscriberThrows_ZeroInterval_RequeuesToTopicTail_AndStores()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe")));

        await Drive(Worker(consumer, ImmutableList.Create(TimeSpan.Zero)), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.Topic, topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MalformedBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.Garbage());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_subscriber.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task NullBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.NullBody());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_subscriber.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MultipleDeliveries_ProcessedInOrder_EachStored()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("a"), 10), Deliveries.Delivery(new TestEvent("b"), 11));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("b", _subscriber.Received?.Name);
        Assert.Equal(new[] { 10L, 11L }, consumer.Stored.Select(offset => offset.Offset.Value));
    }

    [Fact]
    public async Task UnreadableEnvelope_RelaysToFaultTopic()
    {
        // the body parses and the subscriber runs, but the envelope has no trace headers: the error
        // handler cannot read the retry count and reports Faulted — the delivery must end parked.
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.MissingTrace(new TestEvent("pepe")));

        await Drive(Worker(consumer), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task Tombstone_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.Tombstone());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_subscriber.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task EmptyAggregateConsumers_IsNotFiltered_AndRuns()
    {
        // an empty AggregateConsumers header targets nobody in particular: the delivery is for
        // everyone, so the subscriber must run — only a non-empty list excluding this group filters.
        ConsumerFake consumer = new(Deliveries.Delivery(new TestEvent("pepe"), consumers: ""));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
    }
}
