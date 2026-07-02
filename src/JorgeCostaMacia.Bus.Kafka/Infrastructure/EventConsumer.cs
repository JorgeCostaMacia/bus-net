using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one event subscriber in the application lifecycle. On start it launches the
/// configured number of consumer loops on the subscriber's topic; each delivery rebuilds the context
/// from the transport's headers, is handled in its own service scope and acked by committing the
/// offset. On shutdown the loops are cancelled and the consumers closed gracefully.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventConsumer<TEvent, TEventSubscriber> : IHostedService
    where TEvent : Domain.Event
    where TEventSubscriber : IEventSubscriber<TEvent, EventContext<TEvent>, Transport>
{
    private readonly IHandlerConfiguration _configuration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly List<Task> _consumers = [];
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its subscriber configuration and the scope factory.</summary>
    /// <param name="configuration">The subscriber's consumer configuration.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    public EventConsumer(IHandlerConfiguration configuration, IServiceScopeFactory scopeFactory)
    {
        _configuration = configuration;
        _scopeFactory = scopeFactory;
    }

    /// <summary>Launches the consumer loops — one per configured concurrency slot.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellation = new CancellationTokenSource();

        for (int consumer = 0; consumer < _configuration.Consumers; consumer++)
        {
            _consumers.Add(Task.Run(() => Consume(_cancellation.Token), CancellationToken.None));
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
    /// One consumer loop: consume → handle → commit (the offset commit is the ack). A failed
    /// delivery is not committed; the resilience policy (retry / redelivery / error topic) is built
    /// in the next phase.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        using IConsumer<Null, byte[]> consumer = new ConsumerBuilder<Null, byte[]>(_configuration.ConsumerConfig).Build();

        consumer.Subscribe(_configuration.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Null, byte[]> result = consumer.Consume(cancellationToken);

                await Handle(result, cancellationToken);

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

    private async Task Handle(ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
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

        TEventSubscriber subscriber = scope.ServiceProvider.GetRequiredService<TEventSubscriber>();

        await subscriber.Handle(context, cancellationToken);
    }

    private static Transport CreateTransport(ConsumeResult<Null, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);
}
