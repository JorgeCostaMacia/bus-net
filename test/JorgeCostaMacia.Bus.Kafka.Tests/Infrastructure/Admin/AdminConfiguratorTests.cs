using JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Admin;

public class AdminConfiguratorTests
{
    private static IConfiguration Configuration()
    {
        Dictionary<string, string?> values = new Dictionary<string, string?>()
        {
            ["Bus:Admin:BootstrapServers"] = "bus:9092",
            ["Bus:Admin:SaslUsername"] = "admin",
            ["Bus:Admin:SaslPassword"] = "pass"
        };

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact]
    public void AddCommand_RecordsTheTopicPartitionCount()
    {
        AdminConfigurator configurator = new AdminConfigurator(Configuration());

        configurator.AddCommand<TestCommand>("orders", 5);

        Assert.Equal(5, configurator.Topics["orders"]);
    }

    [Fact]
    public void AddEvent_RecordsTheTopicPartitionCount()
    {
        AdminConfigurator configurator = new AdminConfigurator(Configuration());

        configurator.AddEvent<TestEvent>("orders.created", 3);

        Assert.Equal(3, configurator.Topics["orders.created"]);
    }

    [Fact]
    public void AddCommand_WithoutPartitions_DefersToTheBrokerDefault()
    {
        AdminConfigurator configurator = new AdminConfigurator(Configuration());

        configurator.AddCommand<TestCommand>("orders");

        Assert.Equal(-1, configurator.Topics["orders"]);
    }

    [Fact]
    public void AddTopic_Duplicate_Throws()
    {
        AdminConfigurator configurator = new AdminConfigurator(Configuration());

        configurator.AddCommand<TestCommand>("orders");

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => configurator.AddEvent<TestEvent>("orders"));

        Assert.Contains("orders", exception.Message);
    }

    [Fact]
    public void Constructor_MissingTopicsSection_Throws()
    {
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>()).Build();

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => new AdminConfigurator(configuration));

        Assert.Contains("Bus:Admin", exception.Message);
    }

    [Fact]
    public void TopicsBatchSize_WhenUnset_DefaultsToFifty()
    {
        AdminConfigurator configurator = new AdminConfigurator(Configuration());

        Assert.Equal(AdminConfigurationDefaults.TopicsBatchSize, configurator.TopicsBatchSize);
        Assert.Equal(50, configurator.TopicsBatchSize);
    }

    [Fact]
    public void TopicsBatchSize_WhenConfigured_UsesTheConfiguredValue()
    {
        Dictionary<string, string?> values = new Dictionary<string, string?>()
        {
            ["Bus:Admin:BootstrapServers"] = "bus:9092",
            ["Bus:Admin:SaslUsername"] = "admin",
            ["Bus:Admin:SaslPassword"] = "pass",
            ["Bus:Admin:TopicsBatchSize"] = "20"
        };
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        AdminConfigurator configurator = new AdminConfigurator(configuration);

        Assert.Equal(20, configurator.TopicsBatchSize);
    }
}
