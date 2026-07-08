using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class EventWorkerTests
{
    private sealed class BrokerFailure() : RabbitMQClientException("broker down");

    private readonly ProducerFake _producer = new();
    private readonly RecordingEventSubscriber _subscriber = new();

    private EventWorker<TestEvent, RecordingEventSubscriber> Worker(ConsumerChannelFake channel, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_subscriber)
            .AddScoped<Domain.Events.Errors.EventErrorHandler<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventErrorHandler<TestEvent, RecordingEventSubscriber>(_producer, NullLogger.Instance, Deliveries.EXCHANGE, Deliveries.QUEUE, intervals ?? [], []))
            .AddScoped<Domain.Events.Faults.EventFaultHandler<TestEvent, RecordingEventSubscriber>>(_ =>
                new EventFaultHandler<TestEvent, RecordingEventSubscriber>(_producer, NullLogger.Instance, Deliveries.QUEUE))
            .BuildServiceProvider();

        return new EventWorker<TestEvent, RecordingEventSubscriber>(
            channel,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<EventWorker<TestEvent, RecordingEventSubscriber>>.Instance,
            Deliveries.EXCHANGE,
            Deliveries.QUEUE,
            prefetchCount: 10);
    }

    private async Task Deliver(ConsumerChannelFake channel, EventWorker<TestEvent, RecordingEventSubscriber> worker, params BasicDeliverEventArgs[] deliveries)
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);

        foreach (BasicDeliverEventArgs delivery in deliveries) await channel.DeliverAsync(delivery);

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SubscriberSucceeds_AcksAndProducesNothing()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe")));

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.True(channel.Declared);
        Assert.Equal(Deliveries.QUEUE, channel.ConsumedQueue);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task SubscriberThrows_NoLadder_ParksToErrorQueue_AndAcks()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task SubscriberThrows_ZeroInterval_RepublishesToExchange_AndAcks()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel, [TimeSpan.Zero]), Deliveries.Delivery(new TestEvent("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.EXCHANGE, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task SubscriberThrows_ProducerBrokerFailure_NacksWithRequeue()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        _producer.Failure = new BrokerFailure();
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel, [TimeSpan.Zero]), Deliveries.Delivery(new TestEvent("pepe")));

        Assert.Empty(channel.Acked);
        (ulong deliveryTag, bool requeue) = Assert.Single(channel.Nacked);
        Assert.Equal(10ul, deliveryTag);
        Assert.True(requeue);
    }

    [Fact]
    public async Task MalformedBody_ParksToFaultQueue_AndAcks()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Garbage());

        Assert.Null(_subscriber.Received);
        Assert.Equal($"{Deliveries.QUEUE}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task NullBody_ParksToFaultQueue_AndAcks()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.NullBody());

        Assert.Null(_subscriber.Received);
        Assert.Equal($"{Deliveries.QUEUE}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task TargetedToOtherConsumers_SkipsAndAcks()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe"), consumers: "other.subscriber"));

        Assert.Null(_subscriber.Received);
        Assert.Empty(_producer.Produced);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task TargetedToThisQueue_Processes()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe"), consumers: $"other.subscriber, {Deliveries.QUEUE}"));

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }
}
