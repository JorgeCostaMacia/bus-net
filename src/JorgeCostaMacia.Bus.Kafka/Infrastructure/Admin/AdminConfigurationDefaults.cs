namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;

/// <summary>
/// Default topic-provisioning settings an <see cref="AdminConfiguration"/> falls back to for values
/// the <c>Bus:Admin</c> section does not supply. (Security settings fall back to the producer
/// defaults, where the connection defaults live.)
/// </summary>
public static class AdminConfigurationDefaults
{
    /// <summary>
    /// How many topics are created per <c>CreateTopicsAsync</c> request. Default: <c>50</c> — the
    /// declared topics are created in batches instead of one request for all of them, so provisioning
    /// many topics does not spike the controller on a small cluster.
    /// </summary>
    public const int TopicsBatchSize = 50;
}
