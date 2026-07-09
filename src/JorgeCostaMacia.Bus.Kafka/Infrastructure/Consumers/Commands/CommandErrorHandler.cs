using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Errors;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;

/// <summary>
/// The default implementation of the command's error handler — manages <b>only</b> the error case of
/// a failed command delivery over the context the worker already built (the command deserialized
/// once, reused here): a retryable failure follows the interval ladder (a <c>00:00</c> interval
/// requeues to the topic's tail immediately, envelope cloned and <c>RetryCount</c> incremented; a
/// positive one is parked through the retry scheduler to be produced back at its time — or, with no
/// scheduler registered, parked to <c>.error</c> as terminal, since it cannot be delayed); a terminal
/// failure parks a <see cref="CommandError{TCommand}"/> to the topic's <c>.error</c>. It reports how
/// it left the delivery through <c>Result</c>: a produce failure or a scheduler hiccup leaves it
/// <see cref="ErrorResult.Unhandled"/> (the worker escalates it to the fault handler); only an unreadable envelope
/// reports <see cref="ErrorResult.Faulted"/>, handing the delivery to the fault handler.
/// </summary>
/// <typeparam name="TCommand">The command type this handler manages the failures of.</typeparam>
/// <typeparam name="TCommandHandler">The command handler it is paired with — ties this error handler to its command and handler.</typeparam>
internal sealed class CommandErrorHandler<TCommand, TCommandHandler> : Domain.Commands.Errors.CommandErrorHandler<TCommand, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    private const string ERROR_TOPIC_SUFFIX = ".error";

    private readonly IProducer _producer;
    private readonly IRetryScheduler? _retryScheduler;
    private readonly ILogger _logger;

    private readonly string _topic;
    private readonly string _groupId;
    private readonly ImmutableList<TimeSpan> _retryIntervals;
    private readonly ImmutableList<Type> _retryExcludeExceptionTypes;

    /// <summary>Creates the handler over the outbound producer, the optional retry scheduler, the logger and the command's contract.</summary>
    /// <param name="producer">The outbound gate — every produce (retry requeues, error parking) goes through it.</param>
    /// <param name="retryScheduler">The scheduler parking delayed retries, or <see langword="null"/> when none is registered.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="topic">The Kafka topic — retries requeue to it; terminal failures park to its <c>.error</c>.</param>
    /// <param name="groupId">The consumer group id, stamped on parked failures as the failing group.</param>
    /// <param name="retryIntervals">Delays before each retry — one entry per attempt, <c>00:00</c> requeues immediately (empty means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry — they park directly.</param>
    public CommandErrorHandler(
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
    /// Manages the failed command: a retryable failure requeues or schedules following the interval
    /// ladder; a terminal failure parks a <see cref="CommandError{TCommand}"/> to the error topic.
    /// Reports how it left the delivery through <c>Result</c>. Never throws for control flow: a
    /// produce failure or a scheduler hiccup is <see cref="ErrorResult.Unhandled"/>; an
    /// unreadable envelope is <see cref="ErrorResult.Faulted"/>.
    /// </summary>
    /// <param name="context">The delivery's error context — the typed command, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override async Task Handle(CommandErrorContext<TCommand> context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Retryable(context))
            {
                await ParkError(context, cancellationToken);

                using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToErrorTopic)) _logger.LogError(context.Error, "Handler failed.");

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
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(produce, "Producer failed.");

            Result = ErrorResult.Unhandled;
        }
        catch (Exception broken)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.HandedToFaultHandler)) _logger.LogError(broken, "Handler failed.");

            Result = ErrorResult.Faulted;
        }
    }

    /// <summary>Whether the failed command is retried: the envelope's <c>RetryCount</c> is a valid ladder position with entries left and the exception is not excluded (a corrupt/negative count parks as terminal, not retries).</summary>
    private bool Retryable(CommandErrorContext<TCommand> context)
        => context.RetryCount >= 0
            && context.RetryCount < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(context.Error));

    /// <summary>Requeues the retry at the topic's tail — nothing held in memory, survives a restart.</summary>
    private async Task<ErrorResult> Requeue(CommandErrorContext<TCommand> context, CancellationToken cancellationToken)
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
    private async Task<ErrorResult> Schedule(CommandErrorContext<TCommand> context, CancellationToken cancellationToken)
    {
        if (_retryScheduler is null)
        {
            await ParkError(context, cancellationToken);

            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.RetrySchedulerMissing)) _logger.LogError(context.Error, "Handler failed.");

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
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ScheduleFailed)) _logger.LogError(schedule, "Retry failed.");

            return ErrorResult.Unhandled;
        }
    }

    /// <summary>Parks the handler failure to the error topic: a <see cref="CommandError{TCommand}"/> body built from the context (the typed command reused, not re-deserialized), the envelope cloned with the failure stamped on top. The caller logs the outcome.</summary>
    private async Task ParkError(CommandErrorContext<TCommand> context, CancellationToken cancellationToken)
    {
        CommandError<TCommand> error = CommandError<TCommand>.Create(context, _groupId);

        await _producer.Produce(_topic + ERROR_TOPIC_SUFFIX, new Message<Null, byte[]> { Value = JsonSerializer.SerializeToUtf8Bytes(error), Headers = ErrorHeaders(context) }, cancellationToken);
    }

    /// <summary>The retry's body — the typed command re-serialized.</summary>
    private static byte[] Body(CommandErrorContext<TCommand> context)
        => JsonSerializer.SerializeToUtf8Bytes(context.Message);

    /// <summary>The retry's headers — the envelope cloned with <c>RetryCount</c> incremented.</summary>
    private static Headers RetryHeaders(CommandErrorContext<TCommand> context)
    {
        Headers headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(headers, TransportHeaders.RetryCount, TransportHeaders.ToHeader(context.RetryCount + 1));

        return headers;
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing group, the UTC time) — filterable and reinjectable header-side.</summary>
    private Headers ErrorHeaders(CommandErrorContext<TCommand> context)
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
