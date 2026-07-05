using System.Reflection;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// The bus's single outbound gate over the Kafka client — wraps the shared <c>IProducer</c> and is
/// the one place a message is produced (the <see cref="IBus"/> facade's Send/Publish and the
/// consumers' retries and error/fault parking alike). Stamps the producing host on every message
/// (captured once at construction). A failed produce is logged with the outbound delivery attached
/// (topic, body and envelope — inspectable and reinjectable from the log platform) and rethrown: the
/// caller's task still faults, awaiting still means broker-acked. The producer's lifecycle (flush on
/// shutdown, disposal) is the <c>ProducerWorker</c>'s and the container's.
/// </summary>
internal sealed class Producer : IProducer
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly ILogger<Producer> _logger;

    private readonly string _hostMachineName;
    private readonly string _hostAssembly;
    private readonly string _hostAssemblyVersion;
    private readonly string _hostFrameworkVersion;
    private readonly string _hostBusVersion;
    private readonly string _hostOperatingSystemVersion;

    /// <summary>Creates the gate over the shared Kafka producer and the logger, capturing the host once — it never changes while the process runs.</summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="logger">The logger for produce failures.</param>
    public Producer(IProducer<Null, byte[]> producer, ILogger<Producer> logger)
    {
        _producer = producer;
        _logger = logger;

        AssemblyName? entry = Assembly.GetEntryAssembly()?.GetName();

        _hostMachineName = Environment.MachineName;
        _hostAssembly = entry?.Name ?? "unknown";
        _hostAssemblyVersion = entry?.Version?.ToString() ?? "0.0.0";
        _hostFrameworkVersion = Environment.Version.ToString();
        _hostBusVersion = typeof(Producer).Assembly.GetName().Version?.ToString() ?? "0.0.0";
        _hostOperatingSystemVersion = Environment.OSVersion.ToString();
    }

    /// <inheritdoc />
    public async Task Produce(string topic, Message<Null, byte[]> message, CancellationToken cancellationToken = default)
    {
        Stamp(message);

        try
        {
            await _producer.ProduceAsync(topic, message, cancellationToken);
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

    /// <inheritdoc />
    public Task Produce(IEnumerable<KeyValuePair<string, Message<Null, byte[]>>> messages, CancellationToken cancellationToken = default)
        => Task.WhenAll(messages.Select(message => Produce(message.Key, message.Value, cancellationToken)));

    /// <summary>Stamps the producing host onto the message — re-stamping so a cloned envelope carries the failing consumer's host, not the original sender's.</summary>
    private void Stamp(Message<Null, byte[]> message)
    {
        message.Headers ??= new Headers();

        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostMachineName, TransportHeaders.ToHeader(_hostMachineName));
        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostAssembly, TransportHeaders.ToHeader(_hostAssembly));
        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostAssemblyVersion, TransportHeaders.ToHeader(_hostAssemblyVersion));
        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostFrameworkVersion, TransportHeaders.ToHeader(_hostFrameworkVersion));
        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostBusVersion, TransportHeaders.ToHeader(_hostBusVersion));
        TransportHeaders.Restamp(message.Headers, TransportHeaders.HostOperatingSystemVersion, TransportHeaders.ToHeader(_hostOperatingSystemVersion));
    }
}
