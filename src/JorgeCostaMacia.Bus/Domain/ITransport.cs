namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for the transport a delivered message arrived on — one of the two real objects of a
/// delivery (alongside the <see cref="IMessage"/>), surfaced on a context through
/// <see cref="Contexts.ITransportContext{TTransport}"/>. Deliberately empty: it assumes nothing
/// (no header model, no ack shape). Concrete transports expose whatever they actually provide
/// (e.g. a Kafka transport with topic / partition / offset and typed header getters).
/// </summary>
public interface ITransport { }
