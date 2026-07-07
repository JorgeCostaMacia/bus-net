using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

/// <summary>
/// In-memory double of the RabbitMQ <see cref="IChannel"/> the producer publishes through — records
/// every publish (exchange, routing key, persistence, headers, body) and can be told to fail one. Only
/// the publish path the producer uses is implemented; the rest of the wide channel surface throws.
/// </summary>
internal sealed class ChannelFake : IChannel
{
    /// <summary>A captured publish.</summary>
    public sealed record Publish(string Exchange, string RoutingKey, bool Persistent, string? MessageId, string? CorrelationId, string? Type, string? AppId, string? ContentType, long Timestamp, IReadOnlyDictionary<string, object?>? Headers, ReadOnlyMemory<byte> Body);

    /// <summary>The publishes handed to the channel, in order.</summary>
    public List<Publish> Published { get; } = [];

    /// <summary>An exception to fail every publish with, or <see langword="null"/> to succeed.</summary>
    public Exception? PublishFailure { get; set; }

    // Explicit implementations — the generic constraint (incl. v7's `allows ref struct`) is inherited from the interface, not restated.

    ValueTask IChannel.BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
    {
        if (PublishFailure is not null) return ValueTask.FromException(PublishFailure);

        Published.Add(new Publish(exchange, routingKey, basicProperties.Persistent, basicProperties.MessageId, basicProperties.CorrelationId, basicProperties.Type, basicProperties.AppId, basicProperties.ContentType, basicProperties.Timestamp.UnixTime, basicProperties.Headers as IReadOnlyDictionary<string, object?>, body));

        return ValueTask.CompletedTask;
    }

    ValueTask IChannel.BasicPublishAsync<TProperties>(CachedString exchange, CachedString routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
        => throw new NotSupportedException();

    // --- Unused channel surface — the producer only publishes. ---

    public int ChannelNumber => throw new NotSupportedException();
    public ShutdownEventArgs? CloseReason => throw new NotSupportedException();
    public IAsyncBasicConsumer? DefaultConsumer { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public bool IsClosed => throw new NotSupportedException();
    public bool IsOpen => throw new NotSupportedException();
    public string? CurrentQueue => throw new NotSupportedException();
    public TimeSpan ContinuationTimeout { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public event AsyncEventHandler<BasicAckEventArgs> BasicAcksAsync { add { } remove { } }
    public event AsyncEventHandler<BasicNackEventArgs> BasicNacksAsync { add { } remove { } }
    public event AsyncEventHandler<BasicReturnEventArgs> BasicReturnAsync { add { } remove { } }
    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync { add { } remove { } }
    public event AsyncEventHandler<ShutdownEventArgs> ChannelShutdownAsync { add { } remove { } }
    public event AsyncEventHandler<FlowControlEventArgs> FlowControlAsync { add { } remove { } }

    public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public ValueTask BasicRejectAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task BasicCancelAsync(string consumerTag, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<string> BasicConsumeAsync(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object?>? arguments, IAsyncBasicConsumer consumer, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<BasicGetResult?> BasicGetAsync(string queue, bool autoAck, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task CloseAsync(ushort replyCode, string replyText, bool abort, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task CloseAsync(ShutdownEventArgs reason, bool abort, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task CloseAsync(ShutdownEventArgs reason, bool abort) => throw new NotSupportedException();
    public Task<uint> ConsumerCountAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task ExchangeBindAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object?>? arguments, bool passive, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task ExchangeDeclarePassiveAsync(string exchange, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task ExchangeDeleteAsync(string exchange, bool ifUnused, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task ExchangeUnbindAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public ValueTask<ulong> GetNextPublishSequenceNumberAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<uint> MessageCountAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task QueueBindAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<QueueDeclareOk> QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments, bool passive, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<QueueDeclareOk> QueueDeclarePassiveAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<uint> QueueDeleteAsync(string queue, bool ifUnused, bool ifEmpty, bool noWait, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<uint> QueuePurgeAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task QueueUnbindAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task TxCommitAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task TxRollbackAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task TxSelectAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public void Dispose() { }
}
