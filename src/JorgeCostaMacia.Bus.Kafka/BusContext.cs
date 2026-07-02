using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka;

/// <summary>
/// The package's registration facade: creates the Kafka configurations from the <c>Bus:Producer</c> /
/// <c>Bus:Consumer</c> sections (declared once, never repeated), registers the shared producer
/// (error/log callbacks wired to the logger) with its lifecycle worker — registered first so it stops
/// last, the consumers stop before the final flush — and the <see cref="Bus"/> behind its facades,
/// and lets each context map its messages and handlers through the <see cref="BusContextConfigurator"/>.
/// </summary>
public static class BusContext
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
    public static IServiceCollection AddBusContext(this IServiceCollection services, IConfiguration configuration, Action<BusContextConfigurator> configure)
    {
        KafkaProducerConfiguration producer = CreateKafkaProducerConfiguration(configuration);
        KafkaConsumerConfiguration consumer = CreateKafkaConsumerConfiguration(configuration);

        services.AddSingleton(provider => CreateProducer(provider, producer.ProducerConfig));

        services.AddHostedService<BusProducer>();

        services.AddSingleton<IBus, Infrastructure.Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        BusContextConfigurator configurator = new(services, consumer);

        configure(configurator);

        services.AddSingleton(configurator.Messages);

        return services;
    }

    /// <summary>
    /// Maps the <c>Bus:Producer</c> section onto a <see cref="KafkaProducerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes the producer
    /// configuration).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global producer configuration.</returns>
    /// <exception cref="InvalidOperationException"><c>Bus:Producer:BootstrapServers</c> is missing.</exception>
    private static KafkaProducerConfiguration CreateKafkaProducerConfiguration(IConfiguration configuration)
    {
        KafkaProducerConfiguration producer = configuration.GetSection(PRODUCER_SECTION).Get<KafkaProducerConfiguration>() ?? new KafkaProducerConfiguration();

        if (string.IsNullOrWhiteSpace(producer.BootstrapServers))
        {
            throw new InvalidOperationException($"'{PRODUCER_SECTION}:{nameof(producer.BootstrapServers)}' is null.");
        }

        return producer;
    }

    /// <summary>
    /// Maps the <c>Bus:Consumer</c> section onto a <see cref="KafkaConsumerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes each
    /// consumer's configuration). The connection is validated lazily — a producer-only service needs
    /// no <c>Bus:Consumer</c> section.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global consumer configuration.</returns>
    private static KafkaConsumerConfiguration CreateKafkaConsumerConfiguration(IConfiguration configuration)
        => configuration.GetSection(CONSUMER_SECTION).Get<KafkaConsumerConfiguration>() ?? new KafkaConsumerConfiguration();

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger<BusProducer> logger = provider.GetRequiredService<ILogger<BusProducer>>();

        return new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => KafkaProducerLogger.LogError(logger, error))
            .SetLogHandler((_, log) => KafkaProducerLogger.Log(logger, log))
            .Build();
    }
}
