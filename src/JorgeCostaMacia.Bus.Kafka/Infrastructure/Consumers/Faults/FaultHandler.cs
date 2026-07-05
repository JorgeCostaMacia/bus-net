using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Faults;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Faults;

/// <summary>
/// The default implementation of the fault handler — manages <b>only</b> the fault case: parks a
/// <see cref="FaultMessage"/> to the topic's <c>.fault</c> over the raw, never-deserialized body,
/// the envelope cloned in the headers with the failure stamped on top. It runs in two situations: a
/// malformed delivery (the body or envelope is broken, the handler never ran) and, as the relay,
/// when the error handler could not cope — so a delivery is never silently dropped. It reports how
/// it left the delivery through <c>Result</c>: <see cref="FaultHandlerResult.Parked"/> acks,
/// <see cref="FaultHandlerResult.Unhandled"/> leaves it unacked. Never throws for control flow.
/// </summary>
internal sealed class FaultHandler : Domain.Faults.FaultHandler
{
    private const string FAULT_TOPIC_SUFFIX = ".fault";

    private readonly Bus _bus;
    private readonly ILogger _logger;

    private readonly string _topic;
    private readonly string _groupId;

    /// <summary>Creates the handler over the bus, the logger and the consumer's contract.</summary>
    /// <param name="bus">The bus — the fault parking goes through its internal gate.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="topic">The Kafka topic — faults park to its <c>.fault</c>.</param>
    /// <param name="groupId">The consumer group id, stamped on the parked fault as the failing group.</param>
    public FaultHandler(Bus bus, ILogger logger, string topic, string groupId)
    {
        _bus = bus;
        _logger = logger;
        _topic = topic;
        _groupId = groupId;
    }

    /// <summary>
    /// Parks the delivery to the fault topic: a <see cref="FaultMessage"/> body over the raw bytes —
    /// never deserialized, it is the thing that could not be trusted; the envelope cloned in the
    /// headers with the failure stamped on top. Reports the outcome through <c>Result</c>;
    /// never throws for control flow.
    /// </summary>
    /// <param name="context">The fault context — the raw body as text, the transport and the failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override async Task Handle(FaultContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            FaultMessage fault = FaultMessage.Create(context, _groupId);

            Message<Null, byte[]> message = new()
            {
                Value = JsonSerializer.SerializeToUtf8Bytes(fault),
                Headers = FaultHeaders(context)
            };

            await _bus.Produce(_topic + FAULT_TOPIC_SUFFIX, message, cancellationToken);

            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToFaultTopic)) _logger.LogError(context.Error, "Delivery faulted.");

            Result = FaultHandlerResult.Parked;
        }
        catch (OperationCanceledException)
        {
            Result = FaultHandlerResult.Unhandled;
        }
        catch (Exception park)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(park, "Parking failed.");

            Result = FaultHandlerResult.Unhandled;
        }
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing group, the UTC time) — filterable and reinjectable header-side.</summary>
    private Headers FaultHeaders(FaultContext context)
    {
        Type type = context.Error.GetType();

        Headers headers = context.Transport.CloneHeaders();

        headers.Add(TransportHeaders.ErrorType, Encoding.UTF8.GetBytes(type.FullName ?? type.Name));
        headers.Add(TransportHeaders.ErrorMessage, Encoding.UTF8.GetBytes(context.Error.Message));
        headers.Add(TransportHeaders.ErrorGroupId, Encoding.UTF8.GetBytes(_groupId));
        headers.Add(TransportHeaders.ErrorOccurredAt, Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));

        return headers;
    }
}
