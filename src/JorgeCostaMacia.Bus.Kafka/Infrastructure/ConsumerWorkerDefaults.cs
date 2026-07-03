using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default custom-policy settings (the retry ladder and its exclusions) a consumer falls back to
/// when the configurator is not given a value.
/// </summary>
public static class ConsumerWorkerDefaults
{
    /// <summary>Delays before each retry — <c>00:00</c> requeues immediately. Default: empty (no retries).</summary>
    public static ImmutableList<TimeSpan> RETRY_INTERVALS => [];

    /// <summary>Exception types excluded from retry. Default: empty.</summary>
    public static ImmutableList<Type> RETRY_EXCLUDE_EXCEPTION_TYPES => [];
}
