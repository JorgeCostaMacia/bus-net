namespace JorgeCostaMacia.Bus.Domain;

/// <summary>
/// Marker for the context a handler receives when a message is delivered — the read-only envelope
/// around an inbound message. Composable via the facet interfaces in
/// <c>JorgeCostaMacia.Bus.Domain.Contexts</c>.
/// </summary>
public interface IContext { }
