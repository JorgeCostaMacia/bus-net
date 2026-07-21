using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers.Commands;

public class CommandWorkerTests
{
    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RetrySchedulerFake _scheduler = new RetrySchedulerFake();
    private readonly LifetimeFake _lifetime = new LifetimeFake();
    private readonly BusHealth _health = new BusHealth();
    private readonly RecordingCommandHandler _handler = new RecordingCommandHandler();

    private CommandWorker<TestCommand, RecordingCommandHandler> Worker(ConsumerFake consumer, ImmutableList<TimeSpan>? intervals = null, CommandFaultHandlerBase<TestCommand, RecordingCommandHandler>? faultHandler = null)
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_handler)
            .AddScoped<CommandErrorHandlerBase<TestCommand, RecordingCommandHandler>>(_ =>
                new CommandErrorHandler<TestCommand, RecordingCommandHandler>(_producer, _scheduler, NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId, intervals ?? ImmutableList<TimeSpan>.Empty, ImmutableList<Type>.Empty))
            .AddScoped<CommandFaultHandlerBase<TestCommand, RecordingCommandHandler>>(_ =>
                faultHandler ?? new CommandFaultHandler<TestCommand, RecordingCommandHandler>(_producer, NullLogger.Instance, Deliveries.Topic, Deliveries.GroupId))
            .BuildServiceProvider();

        return new CommandWorker<TestCommand, RecordingCommandHandler>(
            consumer,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<CommandWorker<TestCommand, RecordingCommandHandler>>.Instance,
            _lifetime,
            _health,
            new StartupGate(8),
            new StartupSignal(),
            Deliveries.Topic,
            Deliveries.GroupId);
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
        Assert.Equal(Deliveries.Topic, consumer.SubscribedTopic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.Empty(_producer.Produced);
        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
    }

    [Fact]
    public async Task ConsumedDelivery_ReportsTheBrokersUp()
    {
        // a delivery in hand proves the brokers are reachable: consuming flips a down tracker back
        // up before the handler runs — its failures are the delivery's problem, not the connection's.
        _health.Down();
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        Assert.True(_health.IsUp);
    }

    [Fact]
    public async Task HandlerThrows_NoLadder_ParksToErrorTopic_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.error", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task HandlerThrows_ZeroInterval_RequeuesToTopicTail_AndStores()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer, ImmutableList.Create(TimeSpan.Zero)), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.Topic, topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task HandlerThrowsKeyNotFound_GoesToErrorLane_NotFault()
    {
        // a dictionary miss inside USER code is a handling failure (retry ladder), not a malformed delivery
        _handler.Failure = new KeyNotFoundException("user code dictionary miss");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

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

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task NullBody_ParksToFaultTopic_AndStores()
    {
        ConsumerFake consumer = new(Deliveries.NullBody());

        await Drive(Worker(consumer), consumer);

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task ErrorLaneDown_EscalatesAndParksToFaultTopic()
    {
        // the handler fails AND the retry lane's produce fails (partial outage): the error handler
        // reports Unhandled and the worker escalates — the valid message must end parked to .fault.
        _handler.Failure = new InvalidOperationException("boom");
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_Transport), new DeliveryResult<Null, byte[]>());
        _producer.FailingTopics.Add(Deliveries.Topic);
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")));

        await Drive(Worker(consumer, ImmutableList.Create(TimeSpan.Zero)), consumer);

        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task BothLanesDown_LeavesTheDeliveryUnacked_AndKeepsConsuming()
    {
        // the handler fails and BOTH parking lanes are down (the retry requeue and the .fault park):
        // the delivery cannot be parked anywhere — it must stay unacked (the Critical log is the
        // recovery signal) and the loop must survive to keep processing the partition.
        _handler.Failure = new InvalidOperationException("boom");
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_Transport), new DeliveryResult<Null, byte[]>());
        _producer.FailingTopics.Add(Deliveries.Topic);
        _producer.FailingTopics.Add($"{Deliveries.Topic}.fault");
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("first"), 10), Deliveries.Delivery(new TestCommand("second"), 11));

        await Drive(Worker(consumer, ImmutableList.Create(TimeSpan.Zero)), consumer);

        Assert.Equal("second", _handler.Received?.Name);   // the loop survived the first delivery's double failure
        Assert.Empty(consumer.Stored);                     // nothing acked — no delivery was parked
        Assert.Empty(_producer.Produced);                  // and nothing landed on any lane
    }

    [Fact]
    public async Task FaultHandlerThrows_LeavesTheDeliveryUnacked_WithoutTearingDownTheLoop()
    {
        // the last-resort catch: even the fault handler blowing up must not ack the delivery nor
        // kill the consume loop — the Critical log carries the coordinates for manual recovery.
        _handler.Failure = new InvalidOperationException("boom");
        _producer.Failure = new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_Transport), new DeliveryResult<Null, byte[]>());
        _producer.FailingTopics.Add(Deliveries.Topic);
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("first"), 10), Deliveries.Delivery(new TestCommand("second"), 11));

        await Drive(Worker(consumer, ImmutableList.Create(TimeSpan.Zero), new ThrowingCommandFaultHandler()), consumer);

        Assert.Equal("second", _handler.Received?.Name);
        Assert.Empty(consumer.Stored);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task FatalConsumeError_StopsTheApplication()
    {
        ConsumerFake consumer = new ConsumerFake() { ConsumeFailure = new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Fatal, "fatal", isFatal: true)) };

        await Drive(Worker(consumer), consumer);

        Assert.True(_lifetime.StopRequested);
    }

    [Fact]
    public async Task TransientConsumeError_BacksOffAndKeepsConsuming()
    {
        // a non-fatal ConsumeException is the client reconnecting on its own: the loop backs off and
        // survives — the delivery consumed after the failure is still handled and acked.
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("pepe")))
        {
            FirstConsumeFailure = new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Transport))
        };

        await Drive(Worker(consumer), consumer);

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
        Assert.False(_lifetime.StopRequested);
    }

    [Fact]
    public async Task StopAsync_GracePeriodExpired_AbandonsTheWorker_WithoutDisposingTheConsumer()
    {
        // the shutdown's grace period runs out while the loop is still blocked: the stop returns
        // without failing the host and without closing or disposing the consumer under the live loop
        // — the session timeout evicts it and the process teardown reclaims it.
        ConsumerFake consumer = new ConsumerFake() { Hang = true };
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(consumer);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await consumer.Drained.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        await worker.StopAsync(new CancellationToken(canceled: true));

        Assert.False(consumer.Closed);
        Assert.False(consumer.Disposed);

        consumer.Release();
    }

    [Fact]
    public async Task StoreFails_PartitionLost_KeepsConsuming_AndAcksTheNextDelivery()
    {
        // the partition was reclaimed by another owner between handling and storing: the stale store
        // is swallowed (the new owner redelivers) and the loop keeps consuming its remaining work.
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("first"), 10), Deliveries.Delivery(new TestCommand("second"), 11))
        {
            StoreFailure = new KafkaException(new Error(ErrorCode.Local_State))
        };

        await Drive(Worker(consumer), consumer);

        Assert.Equal("second", _handler.Received?.Name);
        Assert.Equal(11, Assert.Single(consumer.Stored).Offset.Value);
        Assert.False(_lifetime.StopRequested);
    }

    [Fact]
    public async Task StoreFails_UnexpectedError_KeepsConsuming_AndAcksTheNextDelivery()
    {
        // an arbitrary store failure leaves the delivery unacked (a restart redelivers it) without
        // tearing anything down: the loop keeps consuming and acking the deliveries that follow.
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("first"), 10), Deliveries.Delivery(new TestCommand("second"), 11))
        {
            StoreFailure = new InvalidOperationException("store down")
        };

        await Drive(Worker(consumer), consumer);

        Assert.Equal("second", _handler.Received?.Name);
        Assert.Equal(11, Assert.Single(consumer.Stored).Offset.Value);
        Assert.False(_lifetime.StopRequested);
    }

    [Fact]
    public async Task ShutdownDuringFaultPark_ExitsTheLoopCleanly_WithoutStoringTheDelivery()
    {
        // the app shuts down while the fault lane is parking a delivery: the cancellation is rethrown
        // for the loop to exit through — no crash, nothing acked, nothing produced — and the stop
        // still closes the consumer gracefully.
        StoppingCommandFaultHandler faultHandler = new StoppingCommandFaultHandler();
        ConsumerFake consumer = new(Deliveries.Garbage());
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(consumer, faultHandler: faultHandler);
        faultHandler.Stop = () => worker.StopAsync(TestContext.Current.CancellationToken);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        Task stop = await faultHandler.Stopping.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await stop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Empty(consumer.Stored);
        Assert.Empty(_producer.Produced);
        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
        Assert.False(_lifetime.StopRequested);
    }

    [Fact]
    public async Task UnreadableEnvelope_RelaysToFaultTopic()
    {
        // the body parses and the handler runs, but the envelope has no trace headers: the error
        // handler cannot read the retry count and reports Faulted — the delivery must end parked.
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerFake consumer = new(Deliveries.MissingTrace(new TestCommand("pepe")));

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

        Assert.Null(_handler.Received);
        (string topic, _) = Assert.Single(_producer.Produced);
        Assert.Equal($"{Deliveries.Topic}.fault", topic);
        Assert.Equal(10, Assert.Single(consumer.Stored).Offset.Value);
    }

    [Fact]
    public async Task MultipleDeliveries_ProcessedInOrder_EachStored()
    {
        ConsumerFake consumer = new(Deliveries.Delivery(new TestCommand("a"), 10), Deliveries.Delivery(new TestCommand("b"), 11));

        await Drive(Worker(consumer), consumer);

        Assert.Equal("b", _handler.Received?.Name);
        Assert.Equal(new[] { 10L, 11L }, consumer.Stored.Select(offset => offset.Offset.Value));
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
