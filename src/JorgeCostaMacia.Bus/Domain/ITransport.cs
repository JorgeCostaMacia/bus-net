namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for the transport a delivered message arrived on — one of the two real objects a context
/// carries (alongside the <see cref="IMessage"/>); the envelope facets are read-only projections
/// over them. Deliberately empty: it assumes nothing (no header model, no ack shape). Concrete
/// transports expose whatever they actually provide (e.g. a Kafka transport with topic / partition /
/// offset and typed header getters). Transport-agnostic reads go through the typed context facets,
/// not through this.
/// </summary>
public interface ITransport { }
