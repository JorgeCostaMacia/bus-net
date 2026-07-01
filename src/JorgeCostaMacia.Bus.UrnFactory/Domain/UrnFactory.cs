using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.UrnFactory.Domain;

/// <summary>
/// Builds the ordered <c>MessageTypeUrn</c> list for a message type: its own URN plus the URNs of
/// its (non-system) interfaces and base types, in the form <c>urn:message:{FullTypeName}</c>. This
/// gives every message a hierarchical identity, enabling <b>polymorphic routing</b> and
/// <b>versioning</b> across the bus (e.g. subscribing to <c>IEvent</c> to receive every domain event).
/// </summary>
public static class UrnFactory
{
    /// <summary>Builds the URN list for the message type <typeparamref name="TMessage"/>.</summary>
    /// <typeparam name="TMessage">The message type to generate URNs for.</typeparam>
    /// <param name="deep">Maximum number of base types to include; <see langword="null"/> for none. Defaults to 2.</param>
    /// <returns>The ordered URNs of the type, its non-system interfaces and its base types.</returns>
    public static ImmutableList<string> Create<TMessage>(int? deep = 2)
        where TMessage : class
        => Create(typeof(TMessage), deep);

    /// <summary>Builds the URN list for the given <paramref name="type"/>.</summary>
    /// <param name="type">The message type to generate URNs for.</param>
    /// <param name="deep">Maximum number of base types to include; <see langword="null"/> for none. Defaults to 2.</param>
    /// <returns>
    /// The ordered URNs of the type, its non-system interfaces and its base types up to
    /// <see cref="object"/>; an empty list when <paramref name="type"/> is <see langword="null"/>.
    /// </returns>
    public static ImmutableList<string> Create(Type type, int? deep = 2)
    {
        if (type is null) return [];

        HashSet<string> urns = [Urn(type)];

        foreach (Type @interface in type.GetInterfaces().Where(e => e.Namespace is not null && !e.Namespace.StartsWith("System")))
        {
            urns.Add(Urn(@interface));
        }

        Type? baseType = type.BaseType;
        int baseTypeCount = 0;
        while (baseType is not null && baseType != typeof(object) && baseTypeCount < deep)
        {
            urns.Add(Urn(baseType));
            baseType = baseType.BaseType;
            baseTypeCount++;
        }

        return urns.OrderBy(urn => urn.Length).ToImmutableList();
    }

    private static string Urn(Type type) => $"urn:message:{type.FullName ?? type.Name}";
}
