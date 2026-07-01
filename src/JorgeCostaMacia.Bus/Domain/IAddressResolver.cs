namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Resolves the destination address a message type is sent to — the same string address vocabulary
/// the contexts use (<c>MessageDestinationAddress</c>). Transport-agnostic: each transport maps the
/// resolved address to its concrete destination (a Kafka topic, a RabbitMQ exchange/queue). A
/// service-provider abstraction that transports implement and the bus/consumer use to route; the
/// application never calls it directly (it just does <c>bus.Send(message)</c> and the bus resolves).
/// </summary>
public interface IAddressResolver
{
    /// <summary>Resolves the destination address for the given message type.</summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <returns>The destination address (topic / exchange / queue name).</returns>
    string Resolve(Type messageType);

    /// <summary>Resolves the destination address for <typeparamref name="TMessage"/>.</summary>
    /// <typeparam name="TMessage">The message type to resolve.</typeparam>
    /// <returns>The destination address.</returns>
    string Resolve<TMessage>() where TMessage : IMessage => Resolve(typeof(TMessage));
}
