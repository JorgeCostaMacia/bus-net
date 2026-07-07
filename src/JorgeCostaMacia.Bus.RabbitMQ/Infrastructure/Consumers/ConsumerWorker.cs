using System.Text.Json;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// The base consumer hosting one handler over one queue: on start it declares its topology (the
/// message exchange of the right kind, the durable queue bound straight to the exchange, and the
/// durable <c>.error</c> / <c>.fault</c> park queues), sets the prefetch, and subscribes with a push
/// consumer. Each delivery runs in its own service scope with each failure in its own lane: a
/// malformed delivery (undeserializable body, unreadable envelope) goes to the fault handler; every
/// other handling failure goes to the error handler, and on to the fault handler as the relay when it
/// reports <see cref="ErrorResult.Faulted"/>. A dealt-with delivery is acked; an unresolved one is
/// nacked with requeue to redeliver. The channel is not shared (one per worker), so the push loop is
/// safe.
/// </summary>
/// <typeparam name="TContext">The context type the handler receives.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where TContext : IContext
    where THandler : class
{
    private const string ERROR_QUEUE_SUFFIX = ".error";
    private const string FAULT_QUEUE_SUFFIX = ".fault";

    private readonly Domain.IConnection _connection;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly string _exchange;
    private readonly string _exchangeType;
    private readonly string _queue;
    private readonly ushort _prefetchCount;
    private readonly CancellationTokenSource _stopping = new();

    private IChannel? _channel;

    /// <summary>Creates the consumer over the shared connection, the scope factory, the logger and its topology.</summary>
    /// <param name="connection">The shared RabbitMQ connection the worker's channel is opened on.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The message exchange the queue binds to.</param>
    /// <param name="exchangeType">The exchange type — <c>direct</c> for commands, <c>fanout</c> for events.</param>
    /// <param name="queue">The queue this handler consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    protected ConsumerWorker(Domain.IConnection connection, IServiceScopeFactory scopeFactory, ILogger logger, string exchange, string exchangeType, string queue, ushort prefetchCount)
    {
        _connection = connection;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _exchange = exchange;
        _exchangeType = exchangeType;
        _queue = queue;
        _prefetchCount = prefetchCount;
    }

    /// <summary>Deserializes the delivery into the handler's context.</summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>The delivery's context.</returns>
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
        _channel = await _connection.CreateChannelAsync(cancellationToken);

        await _channel.ExchangeDeclareAsync(_exchange, _exchangeType, durable: true, autoDelete: false, cancellationToken: cancellationToken);
        await _channel.QueueDeclareAsync(_queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
        await _channel.QueueBindAsync(_queue, _exchange, routingKey: string.Empty, cancellationToken: cancellationToken);

        await _channel.QueueDeclareAsync(_queue + ERROR_QUEUE_SUFFIX, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
        await _channel.QueueDeclareAsync(_queue + FAULT_QUEUE_SUFFIX, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);

        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);

        AsyncEventingBasicConsumer consumer = new(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        await _channel.BasicConsumeAsync(_queue, autoAck: false, consumer, cancellationToken);
    }

    /// <summary>
    /// One delivery inside its own service scope: build the context, run the handler resolved from the
    /// scope, then ack. A malformed delivery goes to the fault handler; every other handling failure to
    /// the error handler (which relays to the fault handler on <see cref="ErrorResult.Faulted"/>). A
    /// shutdown cancellation leaves the delivery unacked for the broker to requeue.
    /// </summary>
    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        TContext? context = default;

        try
        {
            context = CreateContext(args);

            await Handle(scope.ServiceProvider.GetRequiredService<THandler>(), context, _stopping.Token);

            await Ack(args);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Shutting down — leave the delivery unacked for the broker to requeue on channel close.
        }
        catch (JsonException exception)
        {
            await Fault(scope.ServiceProvider, args, exception);
        }
        catch (KeyNotFoundException exception)
        {
            await Fault(scope.ServiceProvider, args, exception);
        }
        catch (InvalidCastException exception)
        {
            await Fault(scope.ServiceProvider, args, exception);
        }
        catch (Exception exception)
        {
            await Error(scope.ServiceProvider, args, context, exception);
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
        catch (Exception failure)
        {
            _logger.LogCritical(failure, "Error handler failed; nacked with requeue.");

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
        catch (Exception failure)
        {
            _logger.LogCritical(failure, "Fault handler failed; nacked with requeue.");

            await Nack(args);
        }
    }

    /// <summary>Acks the delivery — it was dealt with.</summary>
    private async Task Ack(BasicDeliverEventArgs args)
        => await _channel!.BasicAckAsync(args.DeliveryTag, multiple: false, _stopping.Token);

    /// <summary>Nacks the delivery with requeue — it was not dealt with and redelivers.</summary>
    private async Task Nack(BasicDeliverEventArgs args)
        => await _channel!.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, _stopping.Token);

    /// <summary>Stops consuming and closes the channel.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        if (_channel is not null) await _channel.DisposeAsync();

        _stopping.Dispose();
    }
}
