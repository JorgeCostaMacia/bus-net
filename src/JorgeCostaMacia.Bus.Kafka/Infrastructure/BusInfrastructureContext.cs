using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The package's wiring: creates the Kafka configurations from the <c>Bus:Producer</c> /
/// <c>Bus:Consumer</c> sections (declared once, never repeated) and registers the <see cref="Bus"/>
/// behind <see cref="IBus"/> — the single owner of the producer (error/log callbacks wired to its
/// logger) and of the routing map the contexts fill through the <see cref="BusContextConfigurator"/> —
/// with its lifecycle worker, registered first so it stops last: the consumers stop before the final
/// flush.
/// </summary>
internal static class BusInfrastructureContext
{
    private const string PRODUCER_SECTION = "Bus:Producer";
    private const string CONSUMER_SECTION = "Bus:Consumer";

    /// <summary>
    /// Registers the bus over the application configuration and lets each context map its messages
    /// and handlers through the configurator.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Producer</c> / <c>Bus:Consumer</c> sections.</param>
    /// <param name="configure">Maps the contexts' messages and handlers (e.g. chained <c>Map*BusContext</c> calls).</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration, Action<BusContextConfigurator> configure)
    {
        ProducerConfiguration producerConfiguration = CreateProducerConfiguration(configuration);
        ConsumerConfiguration? consumerConfiguration = CreateConsumerConfiguration(configuration);

        Dictionary<Type, string> messages = [];

        services.AddSingleton(provider => CreateBus(provider, producerConfiguration.ProducerConfig, messages));
        services.AddSingleton<IBus>(static provider => provider.GetRequiredService<Bus>());

        services.AddHostedService<BusWorker>();

        BusContextConfigurator configurator = new(services, consumerConfiguration, messages);

        configure(configurator);

        return services;
    }

    /// <summary>
    /// Maps the <c>Bus:Producer</c> section onto a <see cref="ProducerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes the producer
    /// configuration).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global producer configuration.</returns>
    /// <exception cref="InvalidOperationException">The <c>Bus:Producer</c> section or one of its required values is missing.</exception>
    private static ProducerConfiguration CreateProducerConfiguration(IConfiguration configuration)
    {
        ProducerConfiguration producerConfiguration = configuration.GetSection(PRODUCER_SECTION).Get<ProducerConfiguration>()
            ?? throw new InvalidOperationException($"'{PRODUCER_SECTION}' is null.");

        Validate(PRODUCER_SECTION, nameof(producerConfiguration.BootstrapServers), producerConfiguration.BootstrapServers);
        Validate(PRODUCER_SECTION, nameof(producerConfiguration.SaslUsername), producerConfiguration.SaslUsername);
        Validate(PRODUCER_SECTION, nameof(producerConfiguration.SaslPassword), producerConfiguration.SaslPassword);

        return producerConfiguration;
    }

    /// <summary>
    /// Maps the <c>Bus:Consumer</c> section onto a <see cref="ConsumerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes each
    /// consumer's configuration), or <see langword="null"/> when the section is absent — a
    /// producer-only service maps no handlers and needs no section; the configurator throws when a
    /// handler is mapped without it.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global consumer configuration, or <see langword="null"/> when the section is absent.</returns>
    /// <exception cref="InvalidOperationException">The section is present but one of its required values is missing.</exception>
    private static ConsumerConfiguration? CreateConsumerConfiguration(IConfiguration configuration)
    {
        ConsumerConfiguration? consumerConfiguration = configuration.GetSection(CONSUMER_SECTION).Get<ConsumerConfiguration>();

        if (consumerConfiguration is null)
        {
            return null;
        }

        Validate(CONSUMER_SECTION, nameof(consumerConfiguration.BootstrapServers), consumerConfiguration.BootstrapServers);
        Validate(CONSUMER_SECTION, nameof(consumerConfiguration.SaslUsername), consumerConfiguration.SaslUsername);
        Validate(CONSUMER_SECTION, nameof(consumerConfiguration.SaslPassword), consumerConfiguration.SaslPassword);

        return consumerConfiguration;
    }

    /// <summary>Throws when a required configuration value is missing — the binder does not enforce <see langword="required"/> members.</summary>
    /// <param name="section">The configuration section the value belongs to.</param>
    /// <param name="name">The value's name within the section.</param>
    /// <param name="value">The bound value.</param>
    /// <exception cref="InvalidOperationException">The value is missing.</exception>
    private static void Validate(string section, string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"'{section}:{name}' is null.");
        }
    }

    /// <summary>
    /// Creates the bus over the producer it owns — built here with the error/log callbacks wired to
    /// the bus's logger — and the routing map the configurator fills.
    /// </summary>
    private static Bus CreateBus(IServiceProvider provider, ProducerConfig configuration, IReadOnlyDictionary<Type, string> messages)
    {
        ILogger kafkaLogger = provider.GetRequiredService<ILoggerFactory>().CreateLogger(BusLogger.KafkaCategory);

        IProducer<Null, byte[]> producer = new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => BusLogger.LogError(kafkaLogger, error))
            .SetLogHandler((_, log) => BusLogger.Log(kafkaLogger, log))
            .Build();

        return new Bus(producer, messages, provider.GetRequiredService<ILogger<Bus>>());
    }
}
