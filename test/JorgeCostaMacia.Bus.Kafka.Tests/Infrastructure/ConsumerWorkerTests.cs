using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Faults;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class ConsumerWorkerTests
{
    private const string TOPIC = "orders";
    private const string GROUP_ID = "orders.handler";

    private readonly ProducerFake _producer = new();
    private readonly RetrySchedulerFake _scheduler = new();
    private readonly LifetimeFake _lifetime = new();
    private readonly RecordingCommandHandler _handler = new();

    private CommandWorker<TestCommand, RecordingCommandHandler> Worker(ConsumerFake consumer, ImmutableList<TimeSpan>? intervals = null)
    {
        IServiceProvider provider = new ServiceCollection().AddSingleton(_handler).BuildServiceProvider();

        CommandErrorHandler<TestCommand> errorHandler = new(_producer, _scheduler, NullLogger.Instance, TOPIC, GROUP_ID, intervals ?? [], []);
        FaultHandler faultHandler = new(_producer, NullLogger.Instance, TOPIC, GROUP_ID);

        return new CommandWorker<TestCommand, RecordingCommandHandler>(
            consumer,
            errorHandler,
            faultHandler,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<CommandWorker<TestCommand, RecordingCommandHandler>>.Instance,
            _lifetime,
            TOPIC,
            GROUP_ID);
    }

    private static ConsumeResult<Ignore, byte[]> Delivery(TestCommand command, long offset = 10)
    {
        Headers headers =
        [
            new Header(TransportHeaders.RetryCount, "0"u8.ToArray()),
            new Header(TransportHeaders.AggregateId, Guid.NewGuid().ToByteArray()),
            new Header(TransportHeaders.AggregateCorrelationId, Guid.NewGuid().ToByteArray())
        ];

        return new ConsumeResult<Ignore, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(TOPIC, new Partition(0), new Offset(offset)),
            Message = new Message<Ignore, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(command), Headers = headers }
        };
    }

    private static ConsumeResult<Ignore, byte[]> GarbageDelivery(long offset = 10)
        => new()
        {
            TopicPartitionOffset = new TopicPartitionOffset(TOPIC, new Partition(0), new Offset(offset)),
            Message = new Message<Ignore, byte[]> { Value = "}{ not json"u8.ToArray(), Headers = [] }
        };

    private async Task Drive(CommandWorker<TestCommand, RecordingCommandHandler> worker, ConsumerFake consumer)
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);
        await consumer.Drained.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task HandlerSucceeds_StoresTheOffset_AndProducesNothing()
    {
        ConsumerFake consumer = new(Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(TOPIC, consumer.SubscribedTopic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
    }

    [Fact]
    public async Task HandlerThrows_NoLadder_ParksToErrorTopic_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.error", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task HandlerThrows_ZeroInterval_RequeuesToTopicTail_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer, [TimeSpan.Zero]), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal(TOPIC, topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MalformedBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(GarbageDelivery());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{TOPIC}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MultipleDeliveries_ProcessedInOrder_EachStored()
    {
        ConsumerFake consumer = new(Delivery(new TestCommand("a"), 10), Delivery(new TestCommand("b"), 11));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("b", _handler.Received?.Name);
        Assert.Equal([10L, 11L], consumer.Stored.Select(offset => offset.Offset.Value));
    }
}
