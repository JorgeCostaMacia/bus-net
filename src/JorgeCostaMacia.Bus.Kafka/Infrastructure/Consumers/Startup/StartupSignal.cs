namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Startup;

/// <summary>
/// A one-shot startup signal for a single consumer: raised by the consumer's partition-assignment
/// callback the first time it joins its group (by then its connection is established and
/// authenticated), and awaited by the consumer's startup so it can release its
/// <see cref="StartupGate"/> slot the moment it has connected — rather than waiting for the first
/// message, which may never come on an idle topic. One signal per consumer, shared between its Kafka
/// callback and its worker loop.
/// </summary>
internal sealed class StartupSignal
{
    private readonly TaskCompletionSource _ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>A task that completes the first time the consumer joins its group.</summary>
    public Task Ready => _ready.Task;

    /// <summary>Marks the consumer as connected and joined — idempotent; only the first call has effect.</summary>
    public void MarkReady() => _ready.TrySetResult();
}
