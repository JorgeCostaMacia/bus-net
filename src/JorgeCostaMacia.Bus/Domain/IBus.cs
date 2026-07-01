namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for a message bus, so concrete transports can be registered and declared under a single
/// abstraction (e.g. <c>IBus kafkaBus = …</c>). Implemented through <c>ISenderBus</c> / <c>IPublisherBus</c>.
/// </summary>
public interface IBus { }
