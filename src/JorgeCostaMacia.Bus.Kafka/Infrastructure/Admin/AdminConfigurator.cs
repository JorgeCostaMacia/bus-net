using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Admin;

/// <summary>
/// The topic-provisioning side of the bus's configuration, self-contained: it binds its own
/// <see cref="AdminConfiguration"/> from the <c>Bus:Admin</c> section (a dedicated admin connection) and
/// collects the topics to create at startup via <see cref="AddCommand{TCommand}"/> /
/// <see cref="AddEvent{TEvent}"/> — mirroring the producer's mapping calls, so the two lists read the same
/// and a missing topic is easy to spot. Declaring topics here IS the opt-in: omit the topics configurator
/// entirely to leave provisioning to the broker.
/// </summary>
public sealed class AdminConfigurator
{
    private const string AdminSection = "Bus:Admin";

    private readonly Dictionary<string, int> _topics = new Dictionary<string, int>();
    private readonly AdminConfiguration _adminConfiguration;

    /// <summary>Binds the topic configuration from the <c>Bus:Admin</c> section.</summary>
    /// <param name="configuration">The application configuration.</param>
    /// <exception cref="InvalidOperationException">The <c>Bus:Admin</c> section or one of its required values is missing.</exception>
    internal AdminConfigurator(IConfiguration configuration)
    {
        _adminConfiguration = CreateAdminConfiguration(configuration);
    }

    /// <summary>The Kafka admin client settings composed from the bound configuration.</summary>
    internal AdminClientConfig AdminClientConfig => _adminConfiguration.AdminClientConfig;

    /// <summary>The topic → partition-count map to create (<c>-1</c> = the broker's default partition count).</summary>
    internal IReadOnlyDictionary<string, int> Topics => _topics;

    /// <summary>Declares a command's topic to create at startup.</summary>
    /// <typeparam name="TCommand">The command type — mirrors the producer mapping so both lists read alike.</typeparam>
    /// <param name="topic">The Kafka topic to create.</param>
    /// <param name="partitions">The partition count; <c>-1</c> (the default) defers to the broker's default partition count.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public AdminConfigurator AddCommand<TCommand>(string topic, int partitions = -1)
        where TCommand : Command
        => AddTopic(topic, partitions);

    /// <summary>Declares an event's topic to create at startup.</summary>
    /// <typeparam name="TEvent">The event type — mirrors the producer mapping so both lists read alike.</typeparam>
    /// <param name="topic">The Kafka topic to create.</param>
    /// <param name="partitions">The partition count; <c>-1</c> (the default) defers to the broker's default partition count.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public AdminConfigurator AddEvent<TEvent>(string topic, int partitions = -1)
        where TEvent : Event
        => AddTopic(topic, partitions);

    /// <summary>Records a topic and its partition count, rejecting duplicates.</summary>
    /// <param name="topic">The Kafka topic to create.</param>
    /// <param name="partitions">The partition count (<c>-1</c> = the broker's default).</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    private AdminConfigurator AddTopic(string topic, int partitions)
    {
        if (!_topics.TryAdd(topic, partitions))
        {
            throw new InvalidOperationException($"'{topic}' is already declared as a topic to create.");
        }

        return this;
    }

    /// <summary>
    /// Binds the <c>Bus:Admin</c> section onto a <see cref="AdminConfiguration"/> (the admin connection;
    /// unset security settings fall back to the producer defaults when it composes the admin config).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The topic-provisioning configuration.</returns>
    /// <exception cref="InvalidOperationException">The section or one of its required values is missing.</exception>
    private static AdminConfiguration CreateAdminConfiguration(IConfiguration configuration)
    {
        AdminConfiguration adminConfiguration = configuration.GetSection(AdminSection).Get<AdminConfiguration>()
            ?? throw new InvalidOperationException($"'{AdminSection}' is null.");

        if (string.IsNullOrWhiteSpace(adminConfiguration.BootstrapServers))
        {
            throw new InvalidOperationException($"'{AdminSection}:{nameof(adminConfiguration.BootstrapServers)}' is null.");
        }

        if (string.IsNullOrWhiteSpace(adminConfiguration.SaslUsername))
        {
            throw new InvalidOperationException($"'{AdminSection}:{nameof(adminConfiguration.SaslUsername)}' is null.");
        }

        if (string.IsNullOrWhiteSpace(adminConfiguration.SaslPassword))
        {
            throw new InvalidOperationException($"'{AdminSection}:{nameof(adminConfiguration.SaslPassword)}' is null.");
        }

        return adminConfiguration;
    }
}
