using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default custom-policy settings (topic retry / redelivery) a consumer falls back to when the
/// configurator is not given a value.
/// </summary>
public static class HandlerConfigurationDefaults
{
    /// <summary>Maximum retry requeues to the topic. Default: <c>0</c> (no retries).</summary>
    public const int RETRY_ATTEMPTS = 0;

    /// <summary>Exception types excluded from retries. Default: empty.</summary>
    public static ImmutableList<Type> RETRY_EXCLUDE_EXCEPTION_TYPES => [];

    /// <summary>Delays between scheduled redeliveries. Default: empty (no redeliveries).</summary>
    public static ImmutableList<TimeSpan> REDELIVERY_INTERVALS => [];

    /// <summary>Exception types excluded from redelivery. Default: empty.</summary>
    public static ImmutableList<Type> REDELIVERY_EXCLUDE_EXCEPTION_TYPES => [];
}
