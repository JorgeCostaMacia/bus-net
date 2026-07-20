using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;

/// <summary>
/// Creates the declared topics on the broker at startup — an <c>AdminClient.CreateTopicsAsync</c> over the
/// topics collected by the <see cref="AdminConfigurator"/>, using its dedicated admin connection.
/// Registered first among the hosted services so its blocking <see cref="StartAsync"/> completes before any
/// consumer starts: the topics exist by the time anything subscribes, so there is no "unknown topic" churn.
/// Idempotent — topics that already exist are left untouched. Only the declared topics are created; the
/// derived <c>.error</c>/<c>.fault</c> topics are not, so their later appearance still signals a real
/// failure. Each topic's partition count is the declared one (<c>-1</c> defers to the broker); the
/// replication factor always defers to the broker's <c>default.replication.factor</c>.
/// </summary>
internal sealed class AdminWorker : IHostedService
{
    private readonly AdminClientConfig _adminClientConfig;
    private readonly IReadOnlyDictionary<string, int> _topics;
    private readonly ILogger<AdminWorker> _logger;

    /// <summary>Creates the worker over the admin connection and the topic → partition-count map to create.</summary>
    /// <param name="adminClientConfig">The admin client configuration (the dedicated <c>Bus:Admin</c> connection).</param>
    /// <param name="topics">The topic → partition-count map to create (<c>-1</c> = the broker's default partition count).</param>
    /// <param name="logger">The logger.</param>
    public AdminWorker(AdminClientConfig adminClientConfig, IReadOnlyDictionary<string, int> topics, ILogger<AdminWorker> logger)
    {
        _adminClientConfig = adminClientConfig;
        _topics = topics;
        _logger = logger;
    }

    /// <summary>
    /// Creates the declared topics, idempotently. Runs before the consumers so they never subscribe to a
    /// non-existent topic. A creation error other than "already exists" stops the application — the worker
    /// must not run as if healthy when it could not provision its topics.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    /// <exception cref="CreateTopicsException">A topic could not be created for a reason other than already existing.</exception>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_topics.Count == 0)
        {
            return;
        }

        List<TopicSpecification> specifications = _topics
            .Select(topic => new TopicSpecification()
            {
                Name = topic.Key,
                NumPartitions = topic.Value,
                ReplicationFactor = -1
            })
            .ToList();

        using (IAdminClient adminClient = new AdminClientBuilder(_adminClientConfig).Build())
        {
            try
            {
                await adminClient.CreateTopicsAsync(specifications);
            }
            catch (CreateTopicsException exception) when (exception.Results.All(result => !result.Error.IsError || result.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                // every topic was created or already existed — idempotent, nothing to handle. A real
                // creation failure does not match this filter and propagates as the original exception,
                // stopping startup so the worker does not run as if healthy (fail-fast).
            }
        }

        using (BusLogger.DescriptionContext(BusLoggerDescriptions.TopicsEnsured))
        {
            _logger.LogInformation("Topics ensured.");
        }
    }

    /// <summary>Nothing to stop — provisioning is a one-shot at startup.</summary>
    /// <param name="cancellationToken">A token to cancel shutdown.</param>
    /// <returns>A completed task.</returns>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
