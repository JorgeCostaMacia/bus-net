using System.Collections.Immutable;
using System.Reflection;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The Kafka consumer worker — hosts the consume side in the application lifecycle. On start it
/// launches one consumer loop per handler configuration and concurrency slot; each delivery is
/// dispatched in its own service scope and acked by committing the offset. On shutdown it cancels
/// the loops and closes the consumers gracefully.
/// </summary>
internal sealed class Worker : IHostedService
{
    private static readonly MethodInfo HandleCommandMethod = typeof(Worker).GetMethod(nameof(HandleCommand), BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly MethodInfo HandleEventMethod = typeof(Worker).GetMethod(nameof(HandleEvent), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private readonly IReadOnlyList<IHandlerConfiguration> _handlers;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly List<Task> _consumers = [];
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the worker over the handler configurations and the scope factory.</summary>
    /// <param name="handlers">The per-handler consumer configurations (command handler and event subscriber).</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    public Worker(IEnumerable<IHandlerConfiguration> handlers, IServiceScopeFactory scopeFactory)
    {
        _handlers = handlers.ToList();
        _scopeFactory = scopeFactory;
    }

    /// <summary>Launches the consumer loops — one per handler configuration and concurrency slot.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellation = new CancellationTokenSource();

        foreach (IHandlerConfiguration configuration in _handlers)
        {
            MethodInfo dispatch = Dispatch(configuration);

            for (int consumer = 0; consumer < configuration.Consumers; consumer++)
            {
                _consumers.Add(Task.Run(() => Consume(configuration, dispatch, _cancellation.Token), CancellationToken.None));
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>Cancels the consumer loops and awaits their graceful shutdown.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cancellation is null) return;

        _cancellation.Cancel();

        await Task.WhenAll(_consumers).WaitAsync(cancellationToken);

        _cancellation.Dispose();
        _cancellation = null;
        _consumers.Clear();
    }

    /// <summary>
    /// One consumer loop: consume → dispatch → commit (the offset commit is the ack). A failed
    /// delivery is not committed; the resilience policy (retry / redelivery / error topic) is built
    /// in the next phase.
    /// </summary>
    private async Task Consume(IHandlerConfiguration configuration, MethodInfo dispatch, CancellationToken cancellationToken)
    {
        using IConsumer<Null, byte[]> consumer = new ConsumerBuilder<Null, byte[]>(configuration.ConsumerConfig).Build();

        consumer.Subscribe(configuration.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Null, byte[]> result = consumer.Consume(cancellationToken);

                await (Task)dispatch.Invoke(this, [configuration, result, cancellationToken])!;

                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful stop.
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task HandleCommand<TCommand>(IHandlerConfiguration configuration, ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
        where TCommand : Domain.Command
    {
        Transport transport = CreateTransport(result);
        TCommand message = JsonSerializer.Deserialize<TCommand>(result.Message.Value)!;

        CommandContext<TCommand> context = new(
            message,
            transport,
            transport.GetGuid(TransportHeaders.MessageId),
            transport.GetString(TransportHeaders.MessageType),
            transport.GetStringList(TransportHeaders.MessageTypeUrn),
            transport.GetString(TransportHeaders.MessageDestinationAddress),
            transport.GetStringOrDefault(TransportHeaders.MessageOriginAddress),
            transport.GetDateTime(TransportHeaders.MessageOccurredAt),
            transport.GetGuid(TransportHeaders.ConversationId),
            transport.GetString(TransportHeaders.ConversationAddress),
            transport.GetDateTime(TransportHeaders.ConversationOccurredAt),
            transport.GetStringList(TransportHeaders.AggregateDestinationAddresses),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

        using IServiceScope scope = _scopeFactory.CreateScope();

        ICommandHandler<TCommand, CommandContext<TCommand>, Transport> handler =
            (ICommandHandler<TCommand, CommandContext<TCommand>, Transport>)scope.ServiceProvider.GetRequiredService(configuration.HandlerType);

        await handler.Handle(context, cancellationToken);
    }

    private async Task HandleEvent<TEvent>(IHandlerConfiguration configuration, ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
        where TEvent : Domain.Event
    {
        Transport transport = CreateTransport(result);
        TEvent message = JsonSerializer.Deserialize<TEvent>(result.Message.Value)!;

        EventContext<TEvent> context = new(
            message,
            transport,
            transport.GetGuid(TransportHeaders.MessageId),
            transport.GetString(TransportHeaders.MessageType),
            transport.GetStringList(TransportHeaders.MessageTypeUrn),
            transport.GetString(TransportHeaders.MessageDestinationAddress),
            transport.GetStringOrDefault(TransportHeaders.MessageOriginAddress),
            transport.GetDateTime(TransportHeaders.MessageOccurredAt),
            transport.GetGuid(TransportHeaders.ConversationId),
            transport.GetString(TransportHeaders.ConversationAddress),
            transport.GetDateTime(TransportHeaders.ConversationOccurredAt),
            transport.GetStringList(TransportHeaders.AggregateDestinationAddresses),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));

        using IServiceScope scope = _scopeFactory.CreateScope();

        IEventSubscriber<TEvent, EventContext<TEvent>, Transport> subscriber =
            (IEventSubscriber<TEvent, EventContext<TEvent>, Transport>)scope.ServiceProvider.GetRequiredService(configuration.HandlerType);

        await subscriber.Handle(context, cancellationToken);
    }

    private static MethodInfo Dispatch(IHandlerConfiguration configuration)
        => typeof(ICommand).IsAssignableFrom(configuration.MessageType)
            ? HandleCommandMethod.MakeGenericMethod(configuration.MessageType)
            : HandleEventMethod.MakeGenericMethod(configuration.MessageType);

    private static Transport CreateTransport(ConsumeResult<Null, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);
}
