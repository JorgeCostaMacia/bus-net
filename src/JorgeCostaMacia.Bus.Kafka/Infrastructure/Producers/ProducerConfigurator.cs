using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain.Commands;
using JorgeCostaMacia.Bus.Kafka.Domain.Events;
using Microsoft.Extensions.Configuration;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure.Producers;

/// <summary>
/// The send side of the bus's configuration, self-contained: it binds its own
/// <see cref="ProducerConfiguration"/> from the <c>Bus:Producer</c> section, and maps each message
/// this service sends or publishes to its topic (<see cref="AddCommand{TCommand}"/> /
/// <see cref="AddEvent{TEvent}"/>). It owns the type → topic routing map — the single source of truth
/// for a message's topic — which the bus reads to produce and the consumer side reads to know where
/// to subscribe.
/// </summary>
public sealed class ProducerConfigurator
{
    private const string PRODUCER_SECTION = "Bus:Producer";

    private readonly Dictionary<Type, string> _messages = [];
    private readonly ProducerConfiguration _producerConfiguration;

    /// <summary>Binds the producer configuration from the <c>Bus:Producer</c> section.</summary>
    /// <param name="configuration">The application configuration.</param>
    /// <exception cref="InvalidOperationException">The <c>Bus:Producer</c> section or one of its required values is missing.</exception>
    internal ProducerConfigurator(IConfiguration configuration)
        => _producerConfiguration = CreateProducerConfiguration(configuration);

    /// <summary>The Kafka producer settings composed from the bound configuration.</summary>
    internal ProducerConfig ProducerConfig => _producerConfiguration.ProducerConfig;

    /// <summary>The type → topic routing map the bus produces through and the consumers subscribe from.</summary>
    internal IReadOnlyDictionary<Type, string> Messages => _messages;

    /// <summary>Maps a command this service sends to its topic.</summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="topic">The Kafka topic the command is sent to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ProducerConfigurator AddCommand<TCommand>(string topic)
        where TCommand : Command
    {
        Map(typeof(TCommand), topic);

        return this;
    }

    /// <summary>Maps an event this service publishes to its topic.</summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="topic">The Kafka topic the event is published to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ProducerConfigurator AddEvent<TEvent>(string topic)
        where TEvent : Event
    {
        Map(typeof(TEvent), topic);

        return this;
    }

    /// <summary>Maps a message type to its topic, or throws when the type is already mapped.</summary>
    /// <param name="message">The message type.</param>
    /// <param name="topic">The topic to map it to.</param>
    /// <exception cref="InvalidOperationException">The message type is already mapped to a topic.</exception>
    private void Map(Type message, string topic)
    {
        if (!_messages.TryAdd(message, topic))
        {
            throw new InvalidOperationException($"'{message.FullName}' is already mapped to a topic.");
        }
    }

    /// <summary>
    /// Binds the <c>Bus:Producer</c> section onto a <see cref="ProducerConfiguration"/> (the curated
    /// setting surface; unset values fall back to the defaults when it composes the producer config).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global producer configuration.</returns>
    /// <exception cref="InvalidOperationException">The section or one of its required values is missing.</exception>
    private static ProducerConfiguration CreateProducerConfiguration(IConfiguration configuration)
    {
        ProducerConfiguration producerConfiguration = configuration.GetSection(PRODUCER_SECTION).Get<ProducerConfiguration>()
            ?? throw new InvalidOperationException($"'{PRODUCER_SECTION}' is null.");

        Validate(nameof(producerConfiguration.BootstrapServers), producerConfiguration.BootstrapServers);
        Validate(nameof(producerConfiguration.SaslUsername), producerConfiguration.SaslUsername);
        Validate(nameof(producerConfiguration.SaslPassword), producerConfiguration.SaslPassword);

        return producerConfiguration;
    }

    /// <summary>Throws when a required configuration value is missing — the binder does not enforce <see langword="required"/> members.</summary>
    /// <param name="name">The value's name within the section.</param>
    /// <param name="value">The bound value.</param>
    /// <exception cref="InvalidOperationException">The value is missing.</exception>
    private static void Validate(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"'{PRODUCER_SECTION}:{name}' is null.");
        }
    }
}
