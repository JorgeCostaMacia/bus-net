namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Startup;

/// <summary>
/// Bounds how many consumers open their initial broker connection (the TLS + SASL handshake and group
/// join) at the same time, so a service hosting many consumers does not hit the brokers with every
/// handshake in the same instant at startup — the surge that, on a small cluster, shows up as
/// <c>ApiVersionRequest</c> timeouts and transport failures. A single instance is shared across all
/// the service's consumers: each waits its turn before its first consume and releases it as soon as it
/// has joined its group (or a fallback timeout elapses), so the slot frees when the connection is
/// established, not when the first message arrives — an idle topic never holds a slot.
/// </summary>
internal sealed class StartupGate
{
    private readonly SemaphoreSlim _semaphore;

    /// <summary>Creates the gate bounding the concurrent startup connections.</summary>
    /// <param name="maxConcurrency">The maximum number of consumers connecting at once; clamped to at least 1.</param>
    public StartupGate(int maxConcurrency)
    {
        _semaphore = new SemaphoreSlim(Math.Max(1, maxConcurrency));
    }

    /// <summary>Waits for a startup slot.</summary>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>A task that completes when a slot is available.</returns>
    public Task WaitAsync(CancellationToken cancellationToken) => _semaphore.WaitAsync(cancellationToken);

    /// <summary>Releases a startup slot back to the gate.</summary>
    public void Release() => _semaphore.Release();
}
