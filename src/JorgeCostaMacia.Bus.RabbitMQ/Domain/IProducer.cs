namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The bus's single outbound gate: publishes an already-built message (body + envelope headers) to an
/// exchange with a routing key. It is a <b>singleton</b> holding one long-lived, confirmation-enabled
/// channel per destination exchange — concurrent publishes share the destination's channel safely
/// (the client pipelines them, tracking each confirmation), so the channel count is bounded by the
/// routing map, never by the load. The bus facade and the consumers' error/fault handlers all publish
/// through it, so every outbound byte goes through one place.
/// </summary>
internal interface IProducer
{
    /// <summary>Publishes a message to an exchange with a routing key. A completed task means the broker accepted it (the channel publishes with confirmations); a failure throws.</summary>
    /// <param name="exchange">The exchange to publish to.</param>
    /// <param name="routingKey">The routing key (empty for fanout exchanges).</param>
    /// <param name="body">The raw message body.</param>
    /// <param name="headers">The envelope headers to travel with the message.</param>
    /// <param name="cancellationToken">A token to cancel the publish.</param>
    Task Produce(string exchange, string routingKey, ReadOnlyMemory<byte> body, IReadOnlyDictionary<string, object?> headers, CancellationToken cancellationToken = default);
}
