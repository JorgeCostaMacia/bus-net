using Confluent.Kafka;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// A <see cref="ConsumerBuilder{TKey, TValue}"/> that hands back the client callbacks wired onto it, so
/// a test can invoke them directly instead of provoking librdkafka into firing them. The handlers are
/// the builder's own protected fields; exposing them keeps the callback tests deterministic — no broker,
/// no waiting on a client to notice it cannot connect.
/// </summary>
internal sealed class ConsumerBuilderProbe : ConsumerBuilder<Ignore, byte[]>
{
    /// <summary>Creates the probe over an empty configuration — it is never built into a client.</summary>
    public ConsumerBuilderProbe()
        : base(new ConsumerConfig() { GroupId = "probe" })
    {
    }

    /// <summary>The wired error handler.</summary>
    public Action<IConsumer<Ignore, byte[]>, Error>? Error => ErrorHandler;

    /// <summary>The wired internal-log handler.</summary>
    public Action<IConsumer<Ignore, byte[]>, LogMessage>? Log => LogHandler;

    /// <summary>The wired statistics handler.</summary>
    public Action<IConsumer<Ignore, byte[]>, string>? Statistics => StatisticsHandler;
}
