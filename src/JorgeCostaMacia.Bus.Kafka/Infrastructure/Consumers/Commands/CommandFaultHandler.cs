using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Commands;

/// <summary>
/// The default command fault handler — parks a <see cref="CommandFault"/> to the command topic's
/// <c>.fault</c> over the raw, never-deserialized body, the envelope cloned in the headers with the
/// failure stamped on top. Runs for a malformed command delivery, and as the relay when the command
/// error handler could not cope. Reports through <c>Result</c>: <see cref="FaultResult.Parked"/>
/// acks, <see cref="FaultResult.Unhandled"/> leaves it unacked. Never throws for control flow.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
/// <typeparam name="TCommandHandler">The command handler it is paired with.</typeparam>
internal sealed class CommandFaultHandler<TCommand, TCommandHandler> : Domain.Commands.Faults.CommandFaultHandler<TCommand, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    private const string FAULT_TOPIC_SUFFIX = ".fault";

    private readonly IProducer _producer;
    private readonly ILogger _logger;

    private readonly string _topic;
    private readonly string _groupId;

    /// <summary>Creates the handler over the outbound producer, the logger and the consumer's contract.</summary>
    /// <param name="producer">The outbound gate — the fault parking goes through it.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="topic">The Kafka topic — faults park to its <c>.fault</c>.</param>
    /// <param name="groupId">The consumer group id, stamped on the parked fault as the failing group.</param>
    public CommandFaultHandler(IProducer producer, ILogger logger, string topic, string groupId)
    {
        _producer = producer;
        _logger = logger;
        _topic = topic;
        _groupId = groupId;
    }

    /// <inheritdoc />
    public override async Task Handle(CommandFaultContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            CommandFault fault = CommandFault.Create(context, _groupId);

            Message<Null, byte[]> message = new()
            {
                Value = JsonSerializer.SerializeToUtf8Bytes(fault),
                Headers = FaultHeaders(context)
            };

            await _producer.Produce(_topic + FAULT_TOPIC_SUFFIX, message, cancellationToken);

            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.ParkedToFaultTopic)) _logger.LogError(context.Error, "Delivery faulted.");

            Result = FaultResult.Parked;
        }
        catch (OperationCanceledException)
        {
            Result = FaultResult.Unhandled;
        }
        catch (Exception park)
        {
            using (BusLogger.DescriptionContext(_logger, BusLoggerDescriptions.DeliveryNotAcked)) _logger.LogError(park, "Parking failed.");

            Result = FaultResult.Unhandled;
        }
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing group, the UTC time) — filterable and reinjectable header-side.</summary>
    private Headers FaultHeaders(CommandFaultContext context)
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
