namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for a transport's consume metadata — the per-delivery escape hatch a message context
/// carries (as its <c>Metadata</c>) for transport-specific data and access logic. Deliberately
/// empty: it assumes nothing (no header model, no ack shape). Concrete transports expose whatever
/// they actually provide (e.g. a Kafka metadata with topic / partition / offset and typed header
/// getters). Transport-agnostic reads go through the typed context facets, not through this.
/// </summary>
public interface ITransportContext { }
