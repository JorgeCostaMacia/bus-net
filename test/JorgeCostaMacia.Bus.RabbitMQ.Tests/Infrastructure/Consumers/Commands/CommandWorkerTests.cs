using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class CommandWorkerTests
{
    private sealed class BrokerFailure() : RabbitMQClientException("broker down");

    private readonly ProducerFake _producer = new();
    private readonly RecordingCommandHandler _handler = new();

    private CommandWorker<TestCommand, RecordingCommandHandler> Worker(ConsumerChannelFake channel, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_handler)
            .AddScoped<Domain.Commands.Errors.CommandErrorHandler<TestCommand, RecordingCommandHandler>>(_ =>
                new CommandErrorHandler<TestCommand, RecordingCommandHandler>(_producer, NullLogger.Instance, Deliveries.EXCHANGE, Deliveries.QUEUE, intervals ?? [], []))
            .AddScoped<Domain.Commands.Faults.CommandFaultHandler<TestCommand, RecordingCommandHandler>>(_ =>
                new CommandFaultHandler<TestCommand, RecordingCommandHandler>(_producer, NullLogger.Instance, Deliveries.QUEUE))
            .BuildServiceProvider();

        return new CommandWorker<TestCommand, RecordingCommandHandler>(
            channel,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<CommandWorker<TestCommand, RecordingCommandHandler>>.Instance,
            Deliveries.EXCHANGE,
            Deliveries.QUEUE,
            prefetchCount: 10);
    }

    private async Task Deliver(ConsumerChannelFake channel, CommandWorker<TestCommand, RecordingCommandHandler> worker, params BasicDeliverEventArgs[] deliveries)
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);

        foreach (BasicDeliverEventArgs delivery in deliveries) await channel.DeliverAsync(delivery);

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task HandlerSucceeds_AcksAndProducesNothing()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.True(channel.Declared);
        Assert.Equal(Deliveries.QUEUE, channel.ConsumedQueue);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task HandlerThrows_NoLadder_ParksToErrorQueue_AndAcks()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.error", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task HandlerThrows_ZeroInterval_RepublishesToExchange_AndAcks()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel, [TimeSpan.Zero]), Deliveries.Delivery(new TestCommand("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.EXCHANGE, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task HandlerThrows_ProducerBrokerFailure_NacksWithRequeue()
    {
        _handler.Failure = new InvalidOperationException("boom");
        _producer.Failure = new BrokerFailure();
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel, [TimeSpan.Zero]), Deliveries.Delivery(new TestCommand("pepe")));

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

        Assert.Null(_handler.Received);
        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.QUEUE}.fault", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task NullBody_ParksToFaultQueue_AndAcks()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.NullBody());

        Assert.Null(_handler.Received);
        Assert.Equal($"{Deliveries.QUEUE}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task MultipleDeliveries_EachAckedInOrder()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("a"), 10), Deliveries.Delivery(new TestCommand("b"), 11));

        Assert.Equal("b", _handler.Received?.Name);
        Assert.Equal([10ul, 11ul], channel.Acked);
    }

    [Fact]
    public async Task IgnoresAggregateConsumers_AlwaysProcesses()
    {
        ConsumerChannelFake channel = new();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe"), consumers: "other.handler"));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }
}
