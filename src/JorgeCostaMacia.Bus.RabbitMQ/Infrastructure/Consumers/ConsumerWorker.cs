using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// The base consumer hosting one handler over one queue: on start it opens its channel from the
/// factory, declares its topology (the message exchange of the right kind, the durable queue bound
/// straight to the exchange, and the durable <c>.error</c> / <c>.fault</c> park queues), sets the
/// prefetch, and subscribes with a push consumer. Each delivery runs in its own service scope with
/// each failure in its own lane: a malformed delivery (undeserializable body, unreadable envelope)
/// goes to the fault handler; every other handling failure goes to the error handler, and on to the
/// fault handler as the relay when it reports <see cref="ErrorResult.Faulted"/>. A dealt-with delivery
/// is acked; an unresolved one is nacked with requeue to redeliver. The channel is not shared (one per
/// worker), so the push loop is safe.
/// </summary>
/// <typeparam name="TContext">The context type the handler receives.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where TContext : IContext
    where THandler : IHandler
{
    private const string ERROR_QUEUE_SUFFIX = ".error";
    private const string FAULT_QUEUE_SUFFIX = ".fault";

    private readonly IConsumerChannelFactory _channelFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly string _exchange;
    private readonly string _exchangeType;
    private readonly string _queue;
    private readonly ushort _prefetchCount;
    private readonly CancellationTokenSource _stopping = new();

    private IConsumerChannel? _channel;

    /// <summary>Creates the consumer over the channel factory, the scope factory, the logger and its topology.</summary>
    /// <param name="channelFactory">The factory the worker opens its channel from on start.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The message exchange the queue binds to.</param>
    /// <param name="exchangeType">The exchange type — <c>direct</c> for commands, <c>fanout</c> for events.</param>
    /// <param name="queue">The queue this handler consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    protected ConsumerWorker(IConsumerChannelFactory channelFactory, IServiceScopeFactory scopeFactory, ILogger logger, string exchange, string exchangeType, string queue, ushort prefetchCount)
    {
        _channelFactory = channelFactory;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _exchange = exchange;
        _exchangeType = exchangeType;
        _queue = queue;
        _prefetchCount = prefetchCount;
    }

    /// <summary>Deserializes the delivery into the handler's context.</summary>
    /// <returns>The delivery's context.</returns>
    /// <summary>The queue this worker consumes — the consumer identity used for targeted retries and filtering.</summary>
    protected string Queue => _queue;

    /// <summary>
    /// Consumer-side filtering hook: a filtered delivery is acked and skipped before deserializing the
    /// body. Default: nothing is filtered.
    /// </summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected virtual bool Filtered(BasicDeliverEventArgs args) => false;

    protected abstract TContext CreateContext(BasicDeliverEventArgs args);

    /// <summary>Invokes the handler over the delivery's context.</summary>
    /// <param name="handler">The handler resolved from the delivery's scope.</param>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task Handle(THandler handler, TContext context, CancellationToken cancellationToken);

    /// <summary>Hands a handling failure to the error handler resolved from the delivery's scope, and reports its verdict.</summary>
    /// <param name="services">The delivery's scoped service provider.</param>
    /// <param name="context">The delivery's context (the handler already ran).</param>
    /// <param name="exception">The failure the handler raised.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task<ErrorResult> HandleError(IServiceProvider services, TContext context, Exception exception, CancellationToken cancellationToken);

    /// <summary>Hands a broken (or relayed) delivery to the fault handler resolved from the delivery's scope, and reports its verdict.</summary>
    /// <param name="services">The delivery's scoped service provider.</param>
    /// <param name="body">The delivered raw body, never deserialized.</param>
    /// <param name="transport">The broken delivery's transport.</param>
    /// <param name="exception">The failure that broke the delivery.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task<FaultResult> HandleFault(IServiceProvider services, ReadOnlyMemory<byte> body, Transport transport, Exception exception, CancellationToken cancellationToken);

    /// <summary>Opens the channel, declares the topology (message exchange + queue + <c>.error</c> / <c>.fault</c> park queues), and starts consuming.</summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _channel = await _channelFactory.CreateAsync(cancellationToken);

        await _channel.DeclareAsync(_exchange, _exchangeType, _queue, [_queue + ERROR_QUEUE_SUFFIX, _queue + FAULT_QUEUE_SUFFIX], _prefetchCount, cancellationToken);
        await _channel.ConsumeAsync(_queue, OnReceivedAsync, cancellationToken);
    }

    /// <summary>
    /// One delivery inside its own service scope: build the context, run the handler resolved from the
    /// scope, then ack. The two steps fail into different lanes — a context that cannot be built is a
    /// malformed delivery and goes to the fault handler, while any exception thrown by the handler
    /// itself (whatever its type) goes to the error handler and its retry ladder (which still relays to
    /// the fault handler on <see cref="ErrorResult.Faulted"/>). A shutdown cancellation leaves the
    /// delivery unacked for the broker to requeue.
    /// </summary>
    private async Task OnReceivedAsync(BasicDeliverEventArgs args)
    {
        using IDisposable workerScope = BusLogger.WorkerContext(_exchange, _queue);
        using IDisposable deliveryScope = BusLogger.ConsumerContext(args);

        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        if (Filtered(args))
        {
            await AckQuietly(args);

            return;
        }

        TContext context;

        try
        {
            context = CreateContext(args);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Shutting down — leave the delivery unacked for the broker to requeue on channel close.
            return;
        }
        catch (Exception exception)
        {
            await Fault(scope.ServiceProvider, args, exception);

            return;
        }

        try
        {
            await Handle(scope.ServiceProvider.GetRequiredService<THandler>(), context, _stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Shutting down — leave the delivery unacked for the broker to requeue on channel close.
            return;
        }
        catch (Exception exception)
        {
            await Error(scope.ServiceProvider, args, context, exception);

            return;
        }

        await AckQuietly(args);
    }

    /// <summary>
    /// Acks a delivery whose work is already done (handled or filtered). An ack failure must never
    /// reach the error lane — its re-publish would duplicate the work — so it is only logged: left
    /// unacked, the broker redelivers on channel recovery and the idempotent handler absorbs it.
    /// </summary>
    /// <param name="args">The delivery to ack.</param>
    private async Task AckQuietly(BasicDeliverEventArgs args)
    {
        try
        {
            await Ack(args);
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RedeliveredOnRecovery)) _logger.LogWarning(exception, "Ack failed.");
        }
    }

    /// <summary>
    /// A handling failure: hands the delivery to the error handler and acts on its verdict — a requeue,
    /// schedule or park acks it; a report of <see cref="ErrorResult.Faulted"/> (or a context that never
    /// built) relays to the fault handler; anything else nacks with requeue to redeliver. An error
    /// handler that itself throws leaves the delivery nacked-with-requeue rather than tearing down.
    /// </summary>
    private async Task Error(IServiceProvider services, BasicDeliverEventArgs args, TContext? context, Exception exception)
    {
        ErrorResult outcome;

        try
        {
            outcome = context is not null
                ? await HandleError(services, context, exception, _stopping.Token)
                : ErrorResult.Faulted;
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // shutting down, not a failure: the delivery stays unacked and the broker redelivers it.
            return;
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.NackedWithRequeue)) _logger.LogError(failure, "Error handler failed.");

            await Nack(args);

            return;
        }

        switch (outcome)
        {
            case ErrorResult.Retried or ErrorResult.Scheduled or ErrorResult.Parked:
                await Ack(args);
                break;

            case ErrorResult.Faulted:
                await Fault(services, args, exception);
                break;

            default:
                await Nack(args);
                break;
        }
    }

    /// <summary>
    /// A broken (or relayed) delivery: hands it to the fault handler and acks it when the handler parks
    /// it; an unparked delivery is nacked with requeue to redeliver. Never throws for control flow — a
    /// fault handler that itself fails leaves the delivery nacked-with-requeue rather than tearing down.
    /// </summary>
    private async Task Fault(IServiceProvider services, BasicDeliverEventArgs args, Exception exception)
    {
        try
        {
            FaultResult outcome = await HandleFault(services, args.Body, Transport.Create(args), exception, _stopping.Token);

            if (outcome is FaultResult.Parked) await Ack(args);
            else await Nack(args);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // shutting down, not a failure: the delivery stays unacked and the broker redelivers it.
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.NackedWithRequeue)) _logger.LogError(failure, "Fault handler failed.");

            await Nack(args);
        }
    }

    /// <summary>Acks the delivery — it was dealt with.</summary>
    private async Task Ack(BasicDeliverEventArgs args)
        => await _channel!.AckAsync(args.DeliveryTag, _stopping.Token);

    /// <summary>
    /// Nacks the delivery with requeue — it was not dealt with and redelivers. A requeued delivery
    /// comes back immediately, so a persistent failure would spin hot (× prefetch): the nack waits a
    /// second first (like the Kafka worker's consume backoff), skipped when stopping so shutdown
    /// doesn't linger.
    /// </summary>
    private async Task Nack(BasicDeliverEventArgs args)
    {
        if (!_stopping.IsCancellationRequested) await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);

        await _channel!.NackAsync(args.DeliveryTag, requeue: true, _stopping.Token);
    }

    /// <summary>
    /// Stops consuming and closes the channel. The stop is signalled and the channel disposed, but the
    /// token source is deliberately not disposed: a delivery still in flight reads it (its cancellation
    /// is what makes it leave the delivery unacked to redeliver), and disposing it under that read is a
    /// shutdown-time race for no gain — a worker is an app-lifetime singleton, so the token source holds
    /// nothing worth eagerly reclaiming.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        if (_channel is not null) await _channel.DisposeAsync();

        using (BusLogger.WorkerContext(_exchange, _queue))
        using (BusLogger.DescriptionContext(BusLoggerDescriptions.WorkerStopped)) _logger.LogInformation("Worker stopped.");
    }
}
