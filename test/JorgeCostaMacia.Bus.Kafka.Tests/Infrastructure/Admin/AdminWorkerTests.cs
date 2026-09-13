using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Admin;

/// <summary>
/// The parts of the topic-provisioning worker that need no broker: the no-topics short-circuit and the
/// no-op stop. Everything past the short-circuit builds a real <c>AdminClient</c> against a real
/// controller, so the batching, the idempotency and the "only the declared topics" guarantee are pinned
/// by <c>TopicProvisioningTests</c> in the integration suite instead.
/// </summary>
public class AdminWorkerTests
{
    // Deliberately unroutable: if the worker ever got as far as building its admin client and asking the
    // controller to create something, this address is what would make it fail or hang instead of
    // quietly passing.
    private const string UnreachableBroker = "127.0.0.1:1";

    [Fact]
    public async Task StartAsync_WithNoDeclaredTopics_ShortCircuits_AndNeverReachesTheBroker()
    {
        // declaring no topics IS the opt-out (leave provisioning to the broker), so the worker must
        // return before it builds an admin client — the unroutable bootstrap is what proves it did:
        // reaching CreateTopicsAsync against this address could not complete. The absent log is the
        // observable half: "Topics ensured." sits after the short-circuit, so silence means it returned.
        RecordingLogger<AdminWorker> logger = new RecordingLogger<AdminWorker>();
        AdminWorker worker = new AdminWorker(
            new AdminClientConfig() { BootstrapServers = UnreachableBroker },
            new Dictionary<string, int>(),
            topicsBatchSize: 50,
            logger);

        await worker.StartAsync(TestContext.Current.CancellationToken);

        Assert.Empty(logger.Logged);
    }

    [Fact]
    public async Task StopAsync_IsANoOp_AndLogsNothing()
    {
        // provisioning is a one-shot at startup: there is nothing to wind down, and a stop that logged
        // would be noise on every shutdown.
        RecordingLogger<AdminWorker> logger = new RecordingLogger<AdminWorker>();
        AdminWorker worker = new AdminWorker(
            new AdminClientConfig() { BootstrapServers = UnreachableBroker },
            new Dictionary<string, int>() { ["orders"] = 1 },
            topicsBatchSize: 50,
            logger);

        await worker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Empty(logger.Logged);
    }
}
