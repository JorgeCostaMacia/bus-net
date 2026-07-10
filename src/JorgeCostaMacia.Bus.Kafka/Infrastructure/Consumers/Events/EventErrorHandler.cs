using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using JorgeCostaMacia.Bus.Kafka.Domain.Events.Errors;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Events;

/// <summary>
/// The bus's implementation of the event's error handler — manages <b>only</b> the error case of a
/// failed event delivery over the context the worker already built (the event deserialized once,
/// reused here): a retryable failure follows the interval ladder (a <c>00:00</c> interval requeues
/// to the topic's tail immediately, envelope cloned, <c>RetryCount</c> incremented and the retry
/// re-targeted to this group only — the other subscriber groups already handled the original and
/// filter the retry out; a positive one is parked through the retry scheduler to be produced back at
/// its time — or, with no scheduler registered, parked to <c>.error</c> as terminal, since it cannot
/// be delayed); a terminal failure parks an <see cref="EventError{TEvent}"/> to the topic's
/// <c>.error</c>. It reports how it left the delivery through <c>Result</c>: a produce failure or a
/// scheduler hiccup leaves it <see cref="ErrorResult.Unhandled"/> (the worker escalates it to the fault handler); only
/// an unreadable envelope reports <see cref="ErrorResult.Faulted"/>, handing the delivery to
/// the fault handler.
/// </summary>
/// <typeparam name="TEvent">The event type this handler manages the failures of.</typeparam>
/// <typeparam name="TEventSubscriber">The event subscriber it is paired with — ties this error handler to its event and subscriber.</typeparam>
internal sealed class EventErrorHandler<TEvent, TEventSubscriber> : Domain.Events.Errors.EventErrorHandler<TEvent, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    private const string ERROR_TOPIC_SUFFIX = ".error";

    private readonly IProducer _producer;
    private readonly IRetryScheduler? _retryScheduler;
    private readonly ILogger _logger;

    private readonly string _topic;
    private readonly string _groupId;
    private readonly ImmutableList<TimeSpan> _retryIntervals;
    private readonly ImmutableList<Type> _retryExcludeExceptionTypes;

    /// <summary>Creates the handler over the outbound producer, the optional retry scheduler, the logger and the event's contract.</summary>
    /// <param name="producer">The outbound gate — every produce (retry requeues, error parking) goes through it.</param>
    /// <param name="retryScheduler">The scheduler parking delayed retries, or <see langword="null"/> when none is registered.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="topic">The Kafka topic — retries requeue to it; terminal failures park to its <c>.error</c>.</param>
    /// <param name="groupId">The consumer group id — stamped on the retry's targeting and on parked failures as the failing group.</param>
    /// <param name="retryIntervals">Delays before each retry — one entry per attempt, <c>00:00</c> requeues immediately (empty means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry — they park directly.</param>
    public EventErrorHandler(
        IProducer producer,
        IRetryScheduler? retryScheduler,
        ILogger logger,
        string topic,
        string groupId,
        ImmutableList<TimeSpan> retryIntervals,
        ImmutableList<Type> retryExcludeExceptionTypes)
    {
        _producer = producer;
        _retryScheduler = retryScheduler;
        _logger = logger;
        _topic = topic;
        _groupId = groupId;
        _retryIntervals = retryIntervals;
        _retryExcludeExceptionTypes = retryExcludeExceptionTypes;
    }

    /// <summary>
    /// Manages the failed event: a retryable failure requeues or schedules following the interval
    /// ladder; a terminal failure parks an <see cref="EventError{TEvent}"/> to the error topic.
    /// Reports how it left the delivery through <c>Result</c>. Never throws for control flow: a
    /// produce failure or a scheduler hiccup is <see cref="ErrorResult.Unhandled"/>; an
    /// unreadable envelope is <see cref="ErrorResult.Faulted"/>.
    /// </summary>
    /// <param name="context">The delivery's error context — the typed event, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override async Task Handle(EventErrorContext<TEvent> context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Retryable(context))
            {
                await ParkError(context, cancellationToken);

                using (BusLogger.DescriptionContext(BusLoggerDescriptions.ParkedToErrorTopic)) _logger.LogError(context.Error, "Subscriber failed.");

                Result = ErrorResult.Parked;

                return;
            }

            Result = _retryIntervals[context.RetryCount] > TimeSpan.Zero
                ? await Schedule(context, cancellationToken)
                : await Requeue(context, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Result = ErrorResult.Unhandled;
        }
        catch (ProduceException<Null, byte[]> produce)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(produce, "Producer failed.");

            Result = ErrorResult.Unhandled;
        }
        catch (Exception broken)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.HandedToFaultHandler)) _logger.LogError(broken, "Subscriber failed.");

            Result = ErrorResult.Faulted;
        }
    }

    /// <summary>Whether the failed event is retried: the envelope's <c>RetryCount</c> is a valid ladder position with entries left and the exception is not excluded (a corrupt/negative count parks as terminal, not retries).</summary>
    private bool Retryable(EventErrorContext<TEvent> context)
        => context.RetryCount >= 0
            && context.RetryCount < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(context.Error));

    /// <summary>Requeues the retry at the topic's tail, targeted to this group only — nothing held in memory, survives a restart.</summary>
    private async Task<ErrorResult> Requeue(EventErrorContext<TEvent> context, CancellationToken cancellationToken)
    {
        await _producer.Produce(_topic, new Message<Null, byte[]> { Value = Body(context), Headers = RetryHeaders(context) }, cancellationToken);

        BusLogger.LogRetry(_logger, context.Error, context.RetryCount + 1);

        return ErrorResult.Retried;
    }

    /// <summary>
    /// Parks a delayed retry through the scheduler — produced back to the topic at its time. With no
    /// scheduler registered the failure cannot be delayed, so it parks to the error topic as terminal;
    /// a scheduler that fails reports <see cref="ErrorResult.Unhandled"/> — the worker escalates the delivery to the fault handler.
    /// </summary>
    private async Task<ErrorResult> Schedule(EventErrorContext<TEvent> context, CancellationToken cancellationToken)
    {
        if (_retryScheduler is null)
        {
            await ParkError(context, cancellationToken);

            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RetrySchedulerMissing)) _logger.LogError(context.Error, "Subscriber failed.");

            return ErrorResult.Parked;
        }

        DateTime scheduledAt = DateTime.UtcNow.Add(_retryIntervals[context.RetryCount]);

        try
        {
            await _retryScheduler.Schedule(_topic, _groupId, Body(context), RetryHeaders(context), scheduledAt, cancellationToken);

            BusLogger.LogRetry(_logger, context.Error, context.RetryCount + 1, scheduledAt);

            return ErrorResult.Scheduled;
        }
        catch (OperationCanceledException)
        {
            return ErrorResult.Unhandled;
        }
        catch (Exception schedule)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.ScheduleFailed)) _logger.LogError(schedule, "Retry failed.");

            return ErrorResult.Unhandled;
        }
    }

    /// <summary>Parks the subscriber failure to the error topic: an <see cref="EventError{TEvent}"/> body built from the context (the typed event reused, not re-deserialized), the envelope cloned with the failure stamped on top. The caller logs the outcome.</summary>
    private async Task ParkError(EventErrorContext<TEvent> context, CancellationToken cancellationToken)
    {
        EventError<TEvent> error = EventError<TEvent>.Create(context, _groupId);

        await _producer.Produce(_topic + ERROR_TOPIC_SUFFIX, new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(error, BusSerializer.Options), Headers = ErrorHeaders(context) }, cancellationToken);
    }

    /// <summary>The retry's body — the typed event re-serialized.</summary>
    private static byte[] Body(EventErrorContext<TEvent> context)
        => JsonSerializer.SerializeToUtf8Bytes(context.Message, BusSerializer.Options);

    /// <summary>The retry's headers — the envelope cloned, <c>RetryCount</c> incremented and the retry re-targeted to this group only.</summary>
    private Headers RetryHeaders(EventErrorContext<TEvent> context)
    {
        Headers headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(headers, TransportHeaders.RetryCount, TransportHeaders.ToHeader(context.RetryCount + 1));
        TransportHeaders.Restamp(headers, TransportHeaders.AggregateConsumers, TransportHeaders.ToHeader(_groupId));

        return headers;
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing group, the UTC time) — filterable and reinjectable header-side.</summary>
    private Headers ErrorHeaders(EventErrorContext<TEvent> context)
    {
        Type type = context.Error.GetType();

        Headers headers = context.Transport.CloneHeaders();

        headers.Add(TransportHeaders.ErrorType, TransportHeaders.ToHeader(type.FullName ?? type.Name));
        headers.Add(TransportHeaders.ErrorMessage, TransportHeaders.ToHeader(context.Error.Message));
        headers.Add(TransportHeaders.ErrorGroupId, TransportHeaders.ToHeader(_groupId));
        headers.Add(TransportHeaders.ErrorOccurredAt, TransportHeaders.ToHeader(DateTime.UtcNow.ToString("O")));

        return headers;
    }
}
