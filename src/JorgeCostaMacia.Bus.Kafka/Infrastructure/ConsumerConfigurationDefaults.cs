using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default custom-policy settings (redelivery) a consumer falls back to when the configurator is
/// not given a value.
/// </summary>
public static class ConsumerConfigurationDefaults
{
    /// <summary>Delays before each redelivery — <c>00:00</c> requeues immediately. Default: empty (no redeliveries).</summary>
    public static ImmutableList<TimeSpan> REDELIVERY_INTERVALS => [];

    /// <summary>Exception types excluded from redelivery. Default: empty.</summary>
    public static ImmutableList<Type> REDELIVERY_EXCLUDE_EXCEPTION_TYPES => [];
}
