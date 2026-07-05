using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

/// <summary>
/// The inbound gate over the Kafka client — wraps the built <c>Confluent.Kafka.IConsumer</c> and
/// exposes only what the <see cref="ConsumerWorker{TContext, THandler}"/> loop needs. Built from the
/// ready-made builder (its Kafka settings and logging handlers already wired); one per worker.
/// </summary>
internal sealed class Consumer : IConsumer
{
    private readonly IConsumer<Ignore, byte[]> _consumer;

    /// <summary>Builds the consumer from its ready-made Kafka builder.</summary>
    /// <param name="builder">The consumer builder, with the Kafka settings and logging handlers already wired.</param>
    public Consumer(ConsumerBuilder<Ignore, byte[]> builder) => _consumer = builder.Build();

    /// <inheritdoc />
    public void Subscribe(string topic) => _consumer.Subscribe(topic);

    /// <inheritdoc />
    public ConsumeResult<Ignore, byte[]> Consume(CancellationToken cancellationToken) => _consumer.Consume(cancellationToken);

    /// <inheritdoc />
    public void StoreOffset(ConsumeResult<Ignore, byte[]> result) => _consumer.StoreOffset(result);

    /// <inheritdoc />
    public void Close() => _consumer.Close();

    /// <inheritdoc />
    public void Dispose() => _consumer.Dispose();
}
