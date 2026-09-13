using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// A <see cref="ProducerBuilder{TKey, TValue}"/> that hands back the client callbacks wired onto it, so
/// a test can invoke them directly instead of provoking librdkafka into firing them. The handlers are
/// the builder's own protected fields; exposing them keeps the callback tests deterministic — no broker,
/// no waiting on a client to notice it cannot connect.
/// </summary>
internal sealed class ProducerBuilderProbe : ProducerBuilder<Null, byte[]>
{
    /// <summary>Creates the probe over an empty configuration — it is never built into a client.</summary>
    public ProducerBuilderProbe()
        : base(new ProducerConfig())
    {
    }

    /// <summary>The wired error handler.</summary>
    public Action<IProducer<Null, byte[]>, Error>? Error => ErrorHandler;

    /// <summary>The wired internal-log handler.</summary>
    public Action<IProducer<Null, byte[]>, LogMessage>? Log => LogHandler;

    /// <summary>The wired statistics handler.</summary>
    public Action<IProducer<Null, byte[]>, string>? Statistics => StatisticsHandler;
}
