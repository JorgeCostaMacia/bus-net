using JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;
using JorgeCostaMacia.Bus.Kafka.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.IntegrationTests;

/// <summary>
/// Topic provisioning against a real broker: the declared topics exist, with their declared partition
/// counts, by the time the host has started — the guarantee the admin worker is registered first among
/// the hosted services to make. Every assertion reads the broker's own metadata rather than anything the
/// bus reports about itself.
/// <para>
/// The declarations here live only in the <c>admin</c> map, not mirrored in the producer one: production
/// code mirrors them so the two lists read alike, but provisioning is driven by the admin map alone and
/// these tests are about provisioning.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class TopicProvisioningTests : IClassFixture<KafkaFixture>
{
    private readonly KafkaFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running Kafka container.</param>
    public TopicProvisioningTests(KafkaFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>Each declared topic is created with the partition count it declared.</summary>
    [Fact]
    public async Task Declared_topics_are_created_with_their_declared_partition_count()
    {
        string three = Topic();
        string one = Topic();

        await Provision(_fixture.BuildConfiguration(), admin => admin
            .AddCommand<IntegrationCommand>(three, 3)
            .AddCommand<IntegrationCommand>(one, 1));

        IReadOnlyDictionary<string, int> topics = Broker.Topics(_fixture.BuildConfiguration());

        Assert.Equal(3, topics[three]);
        Assert.Equal(1, topics[one]);
    }

    /// <summary>
    /// Re-declaring an existing topic is a no-op, and specifically a no-op that leaves the existing
    /// topic alone: the second declaration asks for a different partition count, the broker answers
    /// TopicAlreadyExists, the worker swallows exactly that, and the original count survives. A worker
    /// that treated "already exists" as a failure would break every restart.
    /// </summary>
    [Fact]
    public async Task Re_declaring_an_existing_topic_is_idempotent_and_leaves_it_untouched()
    {
        string topic = Topic();

        await Provision(_fixture.BuildConfiguration(), admin => admin.AddCommand<IntegrationCommand>(topic, 2));
        await Provision(_fixture.BuildConfiguration(), admin => admin.AddCommand<IntegrationCommand>(topic, 7));

        Assert.Equal(2, Broker.Topics(_fixture.BuildConfiguration())[topic]);
    }

    /// <summary>
    /// More topics than fit in one batch are all created — including the last, partial batch, which is
    /// the one a wrong range calculation would drop.
    /// </summary>
    [Fact]
    public async Task Topics_beyond_the_batch_size_are_all_created_including_the_partial_last_batch()
    {
        // five topics in batches of two: 2 + 2 + 1, so the final batch is short.
        List<string> declared = new List<string>() { Topic(), Topic(), Topic(), Topic(), Topic() };

        await Provision(_fixture.BuildConfiguration(topicsBatchSize: 2), admin =>
        {
            foreach (string topic in declared)
            {
                admin.AddCommand<IntegrationCommand>(topic, 1);
            }
        });

        IReadOnlyDictionary<string, int> topics = Broker.Topics(_fixture.BuildConfiguration());

        Assert.All(declared, topic => Assert.True(topics.ContainsKey(topic), $"'{topic}' was not created."));
    }

    /// <summary>
    /// A configured batch size below one is clamped. Without the clamp the batch loop would advance by
    /// zero and never terminate, so a test that completes at all is the assertion; that both topics
    /// exist is the confirmation it completed by doing the work.
    /// </summary>
    [Fact]
    public async Task A_batch_size_below_one_is_clamped_instead_of_looping_forever()
    {
        string first = Topic();
        string second = Topic();

        await Provision(_fixture.BuildConfiguration(topicsBatchSize: 0), admin => admin
            .AddCommand<IntegrationCommand>(first, 1)
            .AddCommand<IntegrationCommand>(second, 1));

        IReadOnlyDictionary<string, int> topics = Broker.Topics(_fixture.BuildConfiguration());

        Assert.True(topics.ContainsKey(first));
        Assert.True(topics.ContainsKey(second));
    }

    /// <summary>
    /// The derived park lanes are not provisioned. They are born lazily on the first park, which is what
    /// makes their existence a signal that something actually failed; pre-creating them would erase that.
    /// </summary>
    [Fact]
    public async Task The_derived_error_and_fault_topics_are_not_created()
    {
        string topic = Topic();

        await Provision(_fixture.BuildConfiguration(), admin => admin.AddCommand<IntegrationCommand>(topic, 1));

        IReadOnlyDictionary<string, int> topics = Broker.Topics(_fixture.BuildConfiguration());

        Assert.True(topics.ContainsKey(topic));
        Assert.DoesNotContain($"{topic}.error", topics.Keys);
        Assert.DoesNotContain($"{topic}.fault", topics.Keys);
    }

    /// <summary>
    /// Starts a host whose only job is to provision, then stops it. Starting is what runs the admin
    /// worker, so the topics exist once this returns.
    /// </summary>
    /// <param name="configuration">The bus configuration to start against.</param>
    /// <param name="admin">The topic declarations.</param>
    private static async Task Provision(IConfiguration configuration, Action<AdminConfigurator> admin)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<IntegrationCommand>(Topic()),
            consumer: null,
            admin: admin);

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);
        await host.StopAsync(cancellationToken);
    }

    /// <summary>A topic name unique to this run, so no test's topics can satisfy another's assertions.</summary>
    /// <returns>A fresh topic name.</returns>
    private static string Topic() => $"provisioning-{Guid.NewGuid():N}";
}
