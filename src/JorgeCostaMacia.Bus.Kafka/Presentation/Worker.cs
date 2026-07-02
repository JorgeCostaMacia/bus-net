using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.Presentation;

/// <summary>
/// Hosts the bus in the application lifecycle: starts it (launching the consumers) when the host
/// starts and stops it gracefully on shutdown. Registered as a hosted service, so the developer never
/// calls <see cref="IBus.Start"/>/<see cref="IBus.Stop"/> by hand.
/// </summary>
public sealed class Worker : IHostedService
{
    private readonly IBus _bus;

    /// <summary>Creates the worker over the bus it hosts.</summary>
    /// <param name="bus">The bus whose lifecycle follows the host's.</param>
    public Worker(IBus bus)
    {
        _bus = bus;
    }

    /// <summary>Starts the bus when the host starts.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public Task StartAsync(CancellationToken cancellationToken) => _bus.Start(cancellationToken);

    /// <summary>Stops the bus gracefully when the host shuts down.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    public Task StopAsync(CancellationToken cancellationToken) => _bus.Stop(cancellationToken);
}
