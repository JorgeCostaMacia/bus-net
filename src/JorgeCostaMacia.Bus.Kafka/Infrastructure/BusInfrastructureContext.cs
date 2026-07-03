using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.Kafka.Domain.IBus;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The package's wiring: creates the Kafka configurations from the <c>Bus:Producer</c> /
/// <c>Bus:Consumer</c> sections (declared once, never repeated), registers the shared producer
/// (error/log callbacks wired to the logger) with its lifecycle worker — registered first so it stops
/// last, the consumers stop before the final flush — and the <see cref="Bus"/> behind its facades,
/// and lets each context map its messages and handlers through the <see cref="BusContextConfigurator"/>.
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
        ConsumerConfiguration consumerConfiguration = CreateConsumerConfiguration(configuration);

        services.AddSingleton(provider => CreateProducer(provider, producerConfiguration.ProducerConfig));

        services.AddHostedService<ProducerWorker>();

        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        BusContextConfigurator configurator = new(services, consumerConfiguration);

        configure(configurator);

        services.AddSingleton(configurator.Messages);

        return services;
    }

    /// <summary>
    /// Maps the <c>Bus:Producer</c> section onto a <see cref="ProducerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes the producer
    /// configuration).
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global producer configuration.</returns>
    /// <exception cref="InvalidOperationException">The <c>Bus:Producer</c> section or its <c>BootstrapServers</c> is missing.</exception>
    private static ProducerConfiguration CreateProducerConfiguration(IConfiguration configuration)
    {
        ProducerConfiguration producerConfiguration = configuration.GetSection(PRODUCER_SECTION).Get<ProducerConfiguration>()
            ?? throw new InvalidOperationException($"'{PRODUCER_SECTION}' is null.");

        if (string.IsNullOrWhiteSpace(producerConfiguration.BootstrapServers))
        {
            throw new InvalidOperationException($"'{PRODUCER_SECTION}:{nameof(producerConfiguration.BootstrapServers)}' is null.");
        }

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
    /// <exception cref="InvalidOperationException">The section is present but its <c>BootstrapServers</c> is missing.</exception>
    private static ConsumerConfiguration? CreateConsumerConfiguration(IConfiguration configuration)
    {
        ConsumerConfiguration? consumerConfiguration = configuration.GetSection(CONSUMER_SECTION).Get<ConsumerConfiguration>();

        if (consumerConfiguration is not null && string.IsNullOrWhiteSpace(consumerConfiguration.BootstrapServers))
        {
            throw new InvalidOperationException($"'{CONSUMER_SECTION}:{nameof(consumerConfiguration.BootstrapServers)}' is null.");
        }

        return consumerConfiguration;
    }

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger<ProducerWorker> logger = provider.GetRequiredService<ILogger<ProducerWorker>>();

        return new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => ClientLogger.LogError(logger, error))
            .SetLogHandler((_, log) => ClientLogger.Log(logger, log))
            .Build();
    }
}
