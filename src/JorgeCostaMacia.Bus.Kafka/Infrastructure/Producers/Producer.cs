using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// The bus's single outbound gate over the Kafka client — wraps the shared <c>IProducer</c> and is
/// the one place a message is produced (the <see cref="IBus"/> facade's Send/Publish and the
/// consumers' retries and error/fault parking alike). A failed produce is logged with the outbound
/// delivery attached (topic, body and envelope — inspectable and reinjectable from the log platform)
/// and rethrown: the caller's task still faults, awaiting still means broker-acked. The producer's
/// lifecycle (flush on shutdown, disposal) is the <c>ProducerWorker</c>'s and the container's.
/// </summary>
internal sealed class Producer : IProducer
{
    private readonly Confluent.Kafka.IProducer<Null, byte[]> _producer;
    private readonly ILogger<Producer> _logger;

    /// <summary>Creates the gate over the shared Kafka producer and the logger for produce failures.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="logger">The logger for produce failures.</param>
    public Producer(Confluent.Kafka.IProducer<Null, byte[]> producer, ILogger<Producer> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<DeliveryResult<Null, byte[]>> Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _producer.ProduceAsync(topic, message, cancellationToken);
        }
        catch (ProduceException<Null, byte[]> exception) when (exception.Error.Code == ErrorCode.Local_QueueFull)
        {
            using (BusLogger.ProducerContext(_logger, topic, message))
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ProducerQueueFull))
            {
                _logger.LogError(exception, "Producer failed.");
            }

            throw;
        }
        catch (ProduceException<Null, byte[]> exception)
        {
            using (BusLogger.ProducerContext(_logger, topic, message))
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.SendFaulted))
            {
                _logger.LogError(exception, "Producer failed.");
            }

            throw;
        }
    }
}
