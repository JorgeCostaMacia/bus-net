using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

/// <summary>
/// In-memory inbound gate driving the push consumer deterministically: it captures the callback the
/// worker registers on <see cref="ConsumeAsync"/> and, on <see cref="DeliverAsync"/>, invokes it as the
/// broker would — so a test can push a delivery, await its full processing, and assert. Records the
/// acks and nacks (the ack decision), the consumed queue and the dispose. Serves as its own
/// <see cref="IConsumerChannelFactory"/>, handing itself to the worker.
/// </summary>
internal sealed class ConsumerChannelFake : IConsumerChannel, IConsumerChannelFactory
{
    private Func<BasicDeliverEventArgs, Task>? _onReceived;
    private Func<ShutdownEventArgs?, Task>? _onClosed;

    /// <summary>Whether the worker declared its topology on start.</summary>
    public bool Declared { get; private set; }

    /// <summary>The queue the worker subscribed to.</summary>
    public string? ConsumedQueue { get; private set; }

    /// <summary>Whether the worker disposed the channel on stop.</summary>
    public bool Disposed { get; private set; }

    /// <summary>The delivery tags acked by the worker, in order.</summary>
    public List<ulong> Acked { get; } = [];

    /// <summary>The (delivery tag, requeue) nacks by the worker, in order.</summary>
    public List<(ulong DeliveryTag, bool Requeue)> Nacked { get; } = [];

    /// <inheritdoc />
    public Task<IConsumerChannel> CreateAsync(CancellationToken cancellationToken = default) => Task.FromResult<IConsumerChannel>(this);

    /// <inheritdoc />
    public Task DeclareAsync(string exchange, string exchangeType, string queue, IEnumerable<string> parkQueues, ushort prefetchCount, CancellationToken cancellationToken = default)
    {
        Declared = true;

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ConsumeAsync(string queue, Func<BasicDeliverEventArgs, Task> onReceived, Func<ShutdownEventArgs?, Task> onClosed, CancellationToken cancellationToken = default)
    {
        ConsumedQueue = queue;
        _onReceived = onReceived;
        _onClosed = onClosed;

        return Task.CompletedTask;
    }

    /// <summary>When set, every ack throws it — the broker-drop-at-ack seam.</summary>
    public Exception? AckFailure { get; set; }

    /// <inheritdoc />
    public Task AckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
    {
        if (AckFailure is not null) throw AckFailure;

        Acked.Add(deliveryTag);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
    {
        Nacked.Add((deliveryTag, requeue));

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Disposed = true;

        return ValueTask.CompletedTask;
    }

    /// <summary>Drives a delivery into the worker's registered push callback, as the broker would, and awaits its full processing.</summary>
    /// <param name="args">The delivery to push.</param>
    public Task DeliverAsync(BasicDeliverEventArgs args) => _onReceived?.Invoke(args) ?? Task.CompletedTask;

    /// <summary>Kills the channel under the worker, as the broker would, driving its registered death observer.</summary>
    /// <param name="reason">The shutdown reason, or <see langword="null"/> for a consumer cancellation without one.</param>
    public Task CloseAsync(ShutdownEventArgs? reason = null) => _onClosed?.Invoke(reason) ?? Task.CompletedTask;
}
