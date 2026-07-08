using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;

/// <summary>
/// The default event error handler over the context the worker already built: a retryable failure
/// with a <c>00:00</c> interval re-publishes to the event's exchange (immediate retry, envelope cloned
/// and <c>RetryCount</c> incremented); a positive interval would need a retry scheduler (none on
/// RabbitMQ yet), so it parks as terminal; a terminal failure parks an <see cref="EventError{TEvent}"/>
/// to the <c>{queue}.error</c> queue. Reports through <c>Result</c>: a publish failure leaves it
/// <see cref="ErrorResult.Unhandled"/> (redelivers); an unexpected break reports <see cref="ErrorResult.Faulted"/>.
/// </summary>
/// <typeparam name="TEvent">The event type this handler manages the failures of.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber it is paired with.</typeparam>
internal sealed class EventErrorHandler<TEvent, TEventSubscriber> : Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    private const string ERROR_QUEUE_SUFFIX = ".error";

    private readonly IProducer _producer;
    private readonly ILogger _logger;
    private readonly string _exchange;
    private readonly string _queue;
    private readonly ImmutableList<TimeSpan> _retryIntervals;
    private readonly ImmutableList<Type> _retryExcludeExceptionTypes;

    /// <summary>Creates the handler over the outbound gate, the logger and the consumer's contract.</summary>
    /// <param name="producer">The outbound gate — retries re-publish and parks go through it.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="exchange">The event's exchange — an immediate retry re-publishes to it.</param>
    /// <param name="queue">The consumer queue — terminal failures park to its <c>.error</c>, and it is stamped on the parked error as the failing queue.</param>
    /// <param name="retryIntervals">The retry ladder — one delay per attempt (<c>00:00</c> re-publishes immediately; a positive delay parks, as RabbitMQ has no scheduler yet).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry (inheritance-aware).</param>
    public EventErrorHandler(IProducer producer, ILogger logger, string exchange, string queue, ImmutableList<TimeSpan> retryIntervals, ImmutableList<Type> retryExcludeExceptionTypes)
    {
        _producer = producer;
        _logger = logger;
        _exchange = exchange;
        _queue = queue;
        _retryIntervals = retryIntervals;
        _retryExcludeExceptionTypes = retryExcludeExceptionTypes;
    }

    /// <inheritdoc />
    public override async Task Handle(EventErrorContext<TEvent> context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Retryable(context))
            {
                await ParkError(context, cancellationToken);

                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToErrorQueue)) _logger.LogError(context.Error, "Subscriber failed.");

                Result = ErrorResult.Parked;

                return;
            }

            if (_retryIntervals[context.RetryCount] > TimeSpan.Zero)
            {
                await ParkError(context, cancellationToken);

                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.RetrySchedulerMissing)) _logger.LogError(context.Error, "Subscriber failed.");

                Result = ErrorResult.Parked;

                return;
            }

            await Requeue(context, cancellationToken);

            BusLogger.LogRetry(_logger, context.Error, context.RetryCount + 1);

            Result = ErrorResult.Retried;
        }
        catch (OperationCanceledException)
        {
            Result = ErrorResult.Unhandled;
        }
        catch (RabbitMQClientException produce)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(produce, "Producer failed.");

            Result = ErrorResult.Unhandled;
        }
        catch (Exception broken)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.HandedToFaultHandler)) _logger.LogError(broken, "Error handler failed.");

            Result = ErrorResult.Faulted;
        }
    }

    /// <summary>Whether the failed event is retried: a valid ladder position with entries left and an exception not excluded.</summary>
    private bool Retryable(EventErrorContext<TEvent> context)
        => context.RetryCount >= 0
            && context.RetryCount < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(context.Error));

    /// <summary>Re-publishes the retry to the event's exchange, envelope cloned and retry count incremented.</summary>
    private Task Requeue(EventErrorContext<TEvent> context, CancellationToken cancellationToken)
        => _producer.Produce(_exchange, string.Empty, JsonSerializer.SerializeToUtf8Bytes(context.Message), RetryHeaders(context), cancellationToken);

    /// <summary>Parks the subscriber failure to the error queue: an <see cref="EventError{TEvent}"/> built from the context, published via the default exchange to <c>{queue}.error</c>.</summary>
    private Task ParkError(EventErrorContext<TEvent> context, CancellationToken cancellationToken)
        => _producer.Produce(string.Empty, _queue + ERROR_QUEUE_SUFFIX, JsonSerializer.SerializeToUtf8Bytes(EventError<TEvent>.Create(context, _queue)), ErrorHeaders(context), cancellationToken);

    /// <summary>
    /// The retry's headers — the envelope cloned with <c>RetryCount</c> incremented and the retry
    /// re-targeted to this queue only (<c>AggregateConsumers</c>), so the fanout re-publish is skipped
    /// by every other subscriber instead of being reprocessed by all of them.
    /// </summary>
    private Dictionary<string, object?> RetryHeaders(EventErrorContext<TEvent> context)
    {
        Dictionary<string, object?> headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(headers, TransportHeaders.RetryCount, TransportHeaders.ToHeader(context.RetryCount + 1));
        TransportHeaders.Restamp(headers, TransportHeaders.AggregateConsumers, TransportHeaders.ToHeader(_queue));

        return headers;
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing queue, the UTC time).</summary>
    private Dictionary<string, object?> ErrorHeaders(EventErrorContext<TEvent> context)
    {
        Type type = context.Error.GetType();

        Dictionary<string, object?> headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(headers, TransportHeaders.ErrorType, TransportHeaders.ToHeader(type.FullName ?? type.Name));
        TransportHeaders.Restamp(headers, TransportHeaders.ErrorMessage, TransportHeaders.ToHeader(context.Error.Message));
        TransportHeaders.Restamp(headers, TransportHeaders.ErrorGroupId, TransportHeaders.ToHeader(_queue));
        TransportHeaders.Restamp(headers, TransportHeaders.ErrorOccurredAt, TransportHeaders.ToHeader(DateTime.UtcNow.ToString("O")));

        return headers;
    }
}
