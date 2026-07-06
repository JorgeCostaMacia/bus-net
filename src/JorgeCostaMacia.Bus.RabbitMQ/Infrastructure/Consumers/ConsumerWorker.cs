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
/// message exchange of the right kind, the durable queue, and the binding of the queue straight to
/// the exchange), sets the prefetch, and subscribes with a push consumer. Each delivery runs in its
/// own service scope — the handler is resolved from it, the context built once — and is acked on
/// success or nacked (without requeue) on failure. The channel is not shared (one per worker), so the
/// single-threaded push loop is safe.
/// </summary>
/// <typeparam name="TContext">The context type the handler receives.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where TContext : IContext
    where THandler : class
{
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

    /// <summary>Opens the channel, declares the topology, and starts consuming.</summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _channel = await _connection.CreateChannelAsync(cancellationToken);

        await _channel.ExchangeDeclareAsync(_exchange, _exchangeType, durable: true, autoDelete: false, cancellationToken: cancellationToken);
        await _channel.QueueDeclareAsync(_queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
        await _channel.QueueBindAsync(_queue, _exchange, routingKey: string.Empty, cancellationToken: cancellationToken);
        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);

        AsyncEventingBasicConsumer consumer = new(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        await _channel.BasicConsumeAsync(_queue, autoAck: false, consumer, cancellationToken);
    }

    /// <summary>Handles one delivery: build the context, resolve and run the handler in a fresh scope, then ack; on failure, nack without requeue.</summary>
    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        try
        {
            TContext context = CreateContext(args);

            await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

            THandler handler = scope.ServiceProvider.GetRequiredService<THandler>();

            await Handle(handler, context, _stopping.Token);

            await _channel!.BasicAckAsync(args.DeliveryTag, multiple: false, _stopping.Token);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Delivery failed; nacked without requeue.");

            await _channel!.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false, _stopping.Token);
        }
    }

    /// <summary>Stops consuming and closes the channel.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        if (_channel is not null) await _channel.DisposeAsync();

        _stopping.Dispose();
    }
}
