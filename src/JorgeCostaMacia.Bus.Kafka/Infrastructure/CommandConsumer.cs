using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer hosting one command handler in the application lifecycle: one consumer loop on the
/// handler's topic (scale out by running more app instances — the consumer group balances the
/// partitions). Each delivery rebuilds the context from the transport's headers, is handled in its
/// own service scope and acked by storing the offset after handling (committed in the background —
/// the store-offsets pattern). On shutdown the loop is cancelled and the consumer closed gracefully.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandConsumer<TCommand, TCommandHandler> : IHostedService
    where TCommand : Domain.Command
    where TCommandHandler : ICommandHandler<TCommand, CommandContext<TCommand>, Transport>
{
    private readonly IHandlerConfiguration _configuration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<CommandConsumer<TCommand, TCommandHandler>> _logger;
    private Task? _consumer;
    private CancellationTokenSource? _cancellation;

    /// <summary>Creates the consumer over its handler configuration, the scope factory and the logger.</summary>
    /// <param name="configuration">The handler's consumer configuration.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for consumer errors, internal Kafka logs and retries.</param>
    public CommandConsumer(IHandlerConfiguration configuration, IServiceScopeFactory scopeFactory, ILogger<CommandConsumer<TCommand, TCommandHandler>> logger)
    {
        _configuration = configuration;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    /// <summary>Launches the consumer loop.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellation = new CancellationTokenSource();
        CancellationToken token = _cancellation.Token;

        _consumer = Task.Run(() => Consume(token), CancellationToken.None);

        return Task.CompletedTask;
    }

    /// <summary>Cancels the consumer loop and awaits its graceful shutdown.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cancellation is null || _consumer is null) return;

        _cancellation.Cancel();

        await _consumer.WaitAsync(cancellationToken);

        _cancellation.Dispose();
        _cancellation = null;
        _consumer = null;
    }

    /// <summary>
    /// The consumer loop: consume → handle → store the offset (the store is the ack; the background
    /// thread commits it without blocking the loop). The loop only stops on cancellation: a consume
    /// error is logged and retried (the client reconnects on its own), and a failed delivery
    /// (retries exhausted, poison message) is logged and not acked — redelivered after a
    /// restart/rebalance until the resilience policy (redelivery / error topic) lands in the next
    /// phase.
    /// </summary>
    private async Task Consume(CancellationToken cancellationToken)
    {
        ConsumerConfig configuration = _configuration.ConsumerConfig;

        using IDisposable? scope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["Topic"] = _configuration.Topic,
            ["GroupId"] = configuration.GroupId
        });

        using IConsumer<Null, byte[]> consumer = new ConsumerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => LogError(error))
            .SetLogHandler((_, log) => Log(log))
            .Build();

        consumer.Subscribe(_configuration.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<Null, byte[]> result = consumer.Consume(cancellationToken);

                    await Handle(result, cancellationToken);

                    Store(consumer, result);
                }
                catch (ConsumeException exception)
                {
                    _logger.LogError(exception, "Consume failed.");

                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    _logger.LogError(exception, "Handling failed; the delivery is not acked.");
                }
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

    /// <summary>
    /// Handles one delivery with in-process retries: each attempt runs in a fresh service scope; a
    /// failed attempt waits the configured interval and retries with <c>RetryCount</c> incremented.
    /// Excluded exception types (and cancellation) are not retried; when the attempts are exhausted
    /// the exception propagates.
    /// </summary>
    private async Task Handle(ConsumeResult<Null, byte[]> result, CancellationToken cancellationToken)
    {
        Transport transport = CreateTransport(result);
        CommandContext<TCommand> context = CreateContext(result, transport);

        using IDisposable? logging = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = context.MessageId,
            ["ConversationId"] = context.ConversationId,
            ["AggregateId"] = context.AggregateId,
            ["AggregateCorrelationId"] = context.AggregateCorrelationId
        });

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                using IServiceScope scope = _scopeFactory.CreateScope();

                TCommandHandler handler = scope.ServiceProvider.GetRequiredService<TCommandHandler>();

                await handler.Handle(context, cancellationToken);

                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException
                && attempt < _configuration.RetryIntervals.Count
                && !_configuration.RetryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(exception)))
            {
                using (_logger.BeginScope(new Dictionary<string, object?>
                {
                    ["Interval"] = _configuration.RetryIntervals[attempt],
                    ["Retry"] = context.RetryCount + 1
                }))
                {
                    _logger.LogWarning(exception, "Handling failed; retrying.");
                }

                await Task.Delay(_configuration.RetryIntervals[attempt], cancellationToken);

                context = context with { RetryCount = context.RetryCount + 1 };
            }
        }
    }

    private void Store(IConsumer<Null, byte[]> consumer, ConsumeResult<Null, byte[]> result)
    {
        try
        {
            consumer.StoreOffset(result);
        }
        catch (KafkaException exception) when (exception.Error.Code == ErrorCode.Local_State)
        {
            _logger.LogWarning("Partition lost in a rebalance; its new owner will handle the message again.");
        }
    }

    private void LogError(Error error)
    {
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["@Error"] = error
        }))
        {
            _logger.LogError("Consumer error.");
        }
    }

    private void Log(LogMessage log)
    {
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["Name"] = log.Name,
            ["Facility"] = log.Facility
        }))
        {
            _logger.Log((LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging), "{Message}", log.Message);
        }
    }

    private static Transport CreateTransport(ConsumeResult<Null, byte[]> result)
        => new(
            result.Message.Headers.ToImmutableList(),
            result.Topic,
            result.Partition,
            result.Offset,
            result.LeaderEpoch,
            result.Message.Timestamp);

    private static CommandContext<TCommand> CreateContext(ConsumeResult<Null, byte[]> result, Transport transport)
        => new(
            JsonSerializer.Deserialize<TCommand>(result.Message.Value)!,
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
            transport.GetStringList(TransportHeaders.AggregateConsumers),
            transport.GetGuid(TransportHeaders.AggregateId),
            transport.GetGuid(TransportHeaders.AggregateCorrelationId),
            transport.GetDateTime(TransportHeaders.AggregateOccurredAt),
            transport.GetInt(TransportHeaders.RetryCount),
            transport.GetInt(TransportHeaders.RedeliveryCount));
}
