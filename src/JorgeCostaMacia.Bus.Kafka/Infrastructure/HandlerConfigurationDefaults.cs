using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Default custom-policy settings applied to a <see cref="HandlerConfiguration"/> when not supplied.
/// </summary>
public static class HandlerConfigurationDefaults
{
    /// <summary>Maximum retry requeues to the topic. Default: <c>0</c> (no retries).</summary>
    public const int RETRY_ATTEMPTS = 0;

    /// <summary>Exception types excluded from retries. Default: empty.</summary>
    public static ImmutableList<Type> RETRY_EXCLUDE_EXCEPTION_TYPES => [];

    /// <summary>Maximum redelivery attempts. Default: <c>0</c>.</summary>
    public const int REDELIVERY_ATTEMPTS = 0;

    /// <summary>Exception types excluded from redelivery. Default: empty.</summary>
    public static ImmutableList<Type> REDELIVERY_EXCLUDE_EXCEPTION_TYPES => [];
}
