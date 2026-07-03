using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The consumer's failure policy — one place deciding what happens to a failed delivery. A retryable
/// failure follows the interval ladder: a <c>00:00</c> interval requeues to the topic's tail
/// immediately (envelope cloned, <c>RetryCount</c> incremented, the consumer's targeting stamped), a
/// positive one is parked through the retry scheduler to be produced back at its time. A final
/// failure — retries exhausted, an excluded exception, no ladder, or a malformed delivery — is parked
/// to the topic's <c>.error</c> topic with the original envelope plus the failure stamped as headers
/// (Grafana stays the global control point; the error topic is the durable Kafka-side parking).
/// Every outcome that succeeds acks the original delivery; every failure here is logged and leaves it
/// unacked — nothing thrown can escape the consumer loop.
/// </summary>
internal sealed class ConsumerErrorHandler
{
    private const string ERROR_TOPIC_SUFFIX = ".error";

    private readonly Bus _bus;
    private readonly IRetryScheduler? _retryScheduler;
    private readonly ILogger _logger;

    private readonly string _topic;
    private readonly string _groupId;
    private readonly ImmutableList<TimeSpan> _retryIntervals;
    private readonly ImmutableList<Type> _retryExcludeExceptionTypes;

    /// <summary>Creates the policy over the bus, the optional retry scheduler, the logger and the consumer's contract.</summary>
    /// <param name="bus">The bus — every produce (retry requeues, error parking) goes through its internal gate.</param>
    /// <param name="retryScheduler">The scheduler parking delayed retries, or <see langword="null"/> when none is registered — positive intervals are then logged and skipped.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="topic">The Kafka topic the consumer subscribes to — retries requeue to it; final failures park to its <c>.error</c>.</param>
    /// <param name="groupId">The consumer group id, stamped on parked failures as the failing group.</param>
    /// <param name="retryIntervals">Delays before each retry — one entry per attempt, <c>00:00</c> requeues immediately (empty means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry — they park directly.</param>
    public ConsumerErrorHandler(
        Bus bus,
        IRetryScheduler? retryScheduler,
        ILogger logger,
        string topic,
        string groupId,
        ImmutableList<TimeSpan> retryIntervals,
        ImmutableList<Type> retryExcludeExceptionTypes)
    {
        _bus = bus;
        _retryScheduler = retryScheduler;
        _logger = logger;
        _topic = topic;
        _groupId = groupId;
        _retryIntervals = retryIntervals;
        _retryExcludeExceptionTypes = retryExcludeExceptionTypes;
    }

    /// <summary>
    /// Decides a failed delivery's outcome: retryable failures requeue or schedule following the
    /// interval ladder; final failures park to the error topic.
    /// </summary>
    /// <param name="result">The failed delivery.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="target">The concrete consumer's retry targeting, stamped on the requeued envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Whether the delivery is dealt with (the caller then acks it).</returns>
    public async Task<bool> Handle(ConsumeResult<Ignore, byte[]> result, Exception exception, Action<Headers> target, CancellationToken cancellationToken)
    {
        if (!Retryable(result, exception))
        {
            if (!await Park(result, exception, cancellationToken)) return false;

            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToErrorTopic)) _logger.LogError(exception, "Handler failed.");

            return true;
        }

        Transport transport = Transport.Create(result);
        int retry = transport.GetInt(TransportHeaders.RetryCount);
        TimeSpan interval = _retryIntervals[retry];

        Headers headers = transport.CloneHeaders();

        Restamp(headers, TransportHeaders.RetryCount, Encoding.UTF8.GetBytes((retry + 1).ToString()));

        target(headers);

        return interval > TimeSpan.Zero
            ? await Schedule(result, exception, headers, retry, interval, cancellationToken)
            : await Requeue(result, exception, headers, retry, cancellationToken);
    }

    /// <summary>
    /// Parks a malformed delivery to the error topic — its bytes can never be processed, retrying is
    /// pointless.
    /// </summary>
    /// <param name="result">The malformed delivery.</param>
    /// <param name="exception">The deserialization or envelope failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Whether the delivery is parked (the caller then acks it).</returns>
    public async Task<bool> Malformed(ConsumeResult<Ignore, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        if (!await Park(result, exception, cancellationToken)) return false;

        using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToErrorTopic)) _logger.LogError(exception, "Malformed delivery.");

        return true;
    }

    /// <summary>
    /// Whether a failed delivery is retried: the envelope's <c>RetryCount</c> has interval ladder
    /// entries left and the exception type is not excluded.
    /// </summary>
    private bool Retryable(ConsumeResult<Ignore, byte[]> result, Exception exception)
    {
        if (!result.Message.Headers.TryGetLastBytes(TransportHeaders.RetryCount, out byte[] header)) return false;

        return int.TryParse(Encoding.UTF8.GetString(header), out int retries)
            && retries < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(exception));
    }

    /// <summary>
    /// Requeues the retry at the topic's tail — nothing is held in memory and the retry survives a
    /// restart. A failed requeue is logged and leaves the delivery unacked.
    /// </summary>
    private async Task<bool> Requeue(ConsumeResult<Ignore, byte[]> result, Exception exception, Headers headers, int retry, CancellationToken cancellationToken)
    {
        try
        {
            await _bus.Produce(_topic, new Message<Null, byte[]> { Value = result.Message.Value, Headers = headers }, cancellationToken);

            BusLogger.LogRetry(_logger, exception, retry + 1);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ProduceException<Null, byte[]> produce)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(produce, "Producer failed.");

            return false;
        }
    }

    /// <summary>
    /// Parks a delayed retry through the scheduler — produced back to the topic at its time. Without
    /// a registered scheduler, or when scheduling fails, the failure is logged and the delivery left
    /// unacked.
    /// </summary>
    private async Task<bool> Schedule(ConsumeResult<Ignore, byte[]> result, Exception exception, Headers headers, int retry, TimeSpan interval, CancellationToken cancellationToken)
    {
        if (_retryScheduler is null)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.RetrySchedulerMissing)) _logger.LogError(exception, "Handler failed.");

            return false;
        }

        DateTime scheduledAt = DateTime.UtcNow.Add(interval);

        try
        {
            await _retryScheduler.Schedule(_topic, result.Message.Value, headers, scheduledAt, cancellationToken);

            BusLogger.LogRetry(_logger, exception, retry + 1, scheduledAt);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (Exception schedule)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ScheduleFailed)) _logger.LogError(schedule, "Retry failed.");

            return false;
        }
    }

    /// <summary>
    /// Parks a final failure to the topic's <c>.error</c> topic: the original bytes and envelope plus
    /// the failure stamped as headers (exception type/message, the failing group, the UTC time) —
    /// inspectable in Kafka and reinjectable targeted to the failing group. A failed park is logged
    /// and leaves the delivery unacked.
    /// </summary>
    private async Task<bool> Park(ConsumeResult<Ignore, byte[]> result, Exception exception, CancellationToken cancellationToken)
    {
        Type type = exception.GetType();

        Headers headers = Transport.Create(result).CloneHeaders();

        headers.Add(TransportHeaders.ErrorType, Encoding.UTF8.GetBytes(type.FullName ?? type.Name));
        headers.Add(TransportHeaders.ErrorMessage, Encoding.UTF8.GetBytes(exception.Message));
        headers.Add(TransportHeaders.ErrorGroupId, Encoding.UTF8.GetBytes(_groupId));
        headers.Add(TransportHeaders.ErrorOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));

        try
        {
            await _bus.Produce(_topic + ERROR_TOPIC_SUFFIX, new Message<Null, byte[]> { Value = result.Message.Value, Headers = headers }, cancellationToken);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ProduceException<Null, byte[]> produce)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(produce, "Producer failed.");

            return false;
        }
    }

    /// <summary>Replaces every value of a header key with the given one.</summary>
    private static void Restamp(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}
