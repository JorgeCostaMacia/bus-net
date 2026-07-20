using System.Reflection;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure;

/// <summary>
/// Pins the bus log descriptions to their exact text. They feed Loki/Grafana dashboards and alerts, so
/// a reworded, renamed, added or removed description must be a deliberate change — this test fails until
/// the pinned set below is updated to match — and it guards that every description stays distinct.
/// </summary>
public class BusLoggerDescriptionsTests
{
    private static readonly Dictionary<string, string> Expected = new Dictionary<string, string>()
    {
        ["RequeuedToRetry"] = "Requeued to retry.",
        ["ScheduledToRetry"] = "Scheduled to retry.",
        ["ParkedToErrorTopic"] = "Parked to the error topic.",
        ["ParkedToFaultTopic"] = "Parked to the fault topic.",
        ["DeliveryNotAcked"] = "The delivery is not acked.",
        ["DeliveryBuried"] = "The delivery is not parked and not acked; the next commit on its partition buries it — restart before that for a redelivery, or re-inject from the topic.",
        ["ScheduleFailed"] = "The scheduling failed; the delivery is not acked.",
        ["RetrySchedulerMissing"] = "No retry scheduler is registered; parked to the error topic as terminal.",
        ["HandedToFaultHandler"] = "The envelope is unreadable; handed to the fault handler.",
        ["EscalatedToFaultHandler"] = "The error handler failed; escalated to the fault handler.",
        ["SendFaulted"] = "The send faulted.",
        ["ProducerQueueFull"] = "The producer's local queue is full; back-pressure upstream or raise QueueBufferingMaxMessages.",
        ["ConsumeRetried"] = "The consume is retried.",
        ["ConsumeLoopFailed"] = "An unexpected failure in the consume loop; backing off before the next consume.",
        ["WorkerStopped"] = "The worker stopped.",
        ["WorkerAbandoned"] = "The stop grace period expired; the consumer leaves the group by session timeout and is reclaimed at process exit.",
        ["RedeliveredToNewOwner"] = "Lost in a rebalance; the new owner will handle the message again.",
        ["RedeliveryWindowWidened"] = "The stored offsets were not committed; the crash-redelivery window widened.",
        ["ApplicationStopped"] = "The client is unrecoverable; the application stops to restart clean.",
        ["QueuedMessagesMayBeLost"] = "Queued messages may be lost.",
        ["TopicsEnsured"] = "The mapped topics are ensured on the broker before the consumers subscribe."
    };

    [Fact]
    public void Descriptions_MatchTheirPinnedText()
    {
        Dictionary<string, string> actual = Descriptions();

        Assert.Equal(Expected.Count, actual.Count);

        foreach ((string name, string text) in actual)
        {
            Assert.True(Expected.TryGetValue(name, out string? expected), $"Unpinned description '{name}' — add it to the pinned set (and update any Loki/Grafana query that keys on it).");
            Assert.Equal(expected, text);
        }
    }

    [Fact]
    public void Descriptions_AreDistinct()
    {
        string[] values = Descriptions().Values.ToArray();

        Assert.Equal(values.Length, values.Distinct().Count());
    }

    /// <summary>Reads every public string constant off the descriptions holder by reflection.</summary>
    private static Dictionary<string, string> Descriptions()
        => typeof(BusLoggerDescriptions)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field is { IsLiteral: true, IsInitOnly: false } && field.FieldType == typeof(string))
            .ToDictionary(field => field.Name, field => (string)field.GetValue(null)!);
}
