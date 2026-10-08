namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;

/// <summary>
/// Default topic-provisioning settings an <see cref="AdminConfiguration"/> falls back to for values
/// the <c>Bus:Admin</c> section does not supply. (Security settings fall back to the producer
/// defaults, where the connection defaults live.)
/// </summary>
public static class AdminConfigurationDefaults
{
    /// <summary>
    /// How many topics are created per <c>CreateTopicsAsync</c> request. Default: <c>25</c> — the
    /// declared topics are created in batches instead of one request for all of them, so provisioning
    /// many topics does not spike the controller on a small cluster. It was 50 up to 4.1, lowered as a
    /// precaution: it only matters on a service's first deploy, when its topics are new (at eight
    /// partitions each, half the partitions per request); once they exist each batch is a no-op answered
    /// with <c>TopicAlreadyExists</c>, so the smaller batch costs a few round trips at startup and nothing else.
    /// </summary>
    public const int TopicsBatchSize = 25;
}
