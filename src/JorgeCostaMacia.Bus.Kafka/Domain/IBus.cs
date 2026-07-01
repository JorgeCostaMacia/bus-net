using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// The bus — the single entry point for a service: it sends commands (<see cref="ICommandBus"/>),
/// publishes events (<see cref="IEventBus"/>) and owns the connection lifecycle (launch the
/// producers/consumers, stop them gracefully). One injected object handles everything; the
/// per-message / per-handler configuration is held and managed inside it.
/// </summary>
public interface IBus : ICommandBus, IEventBus
{
    /// <summary>Starts the bus: opens connections and launches the consumers.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    Task Start(CancellationToken cancellationToken = default);

    /// <summary>Stops the bus: closes consumers and producers gracefully.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    Task Stop(CancellationToken cancellationToken = default);
}
