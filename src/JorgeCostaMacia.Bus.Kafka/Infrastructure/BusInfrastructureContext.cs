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
        ProducerConfiguration producer = CreateProducerConfiguration(configuration);
        ConsumerConfiguration consumer = CreateConsumerConfiguration(configuration);

        services.AddSingleton(provider => CreateProducer(provider, producer.ProducerConfig));

        services.AddHostedService<ProducerWorker>();

        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<ICommandBus>(static provider => provider.GetRequiredService<IBus>());
        services.AddSingleton<IEventBus>(static provider => provider.GetRequiredService<IBus>());

        BusContextConfigurator configurator = new(services, consumer);

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
    /// <exception cref="InvalidOperationException"><c>Bus:Producer:BootstrapServers</c> is missing.</exception>
    private static ProducerConfiguration CreateProducerConfiguration(IConfiguration configuration)
    {
        ProducerConfiguration producer = configuration.GetSection(PRODUCER_SECTION).Get<ProducerConfiguration>() ?? new ProducerConfiguration();

        if (string.IsNullOrWhiteSpace(producer.BootstrapServers))
        {
            throw new InvalidOperationException($"'{PRODUCER_SECTION}:{nameof(producer.BootstrapServers)}' is null.");
        }

        return producer;
    }

    /// <summary>
    /// Maps the <c>Bus:Consumer</c> section onto a <see cref="ConsumerConfiguration"/> (the
    /// curated setting surface; unset values fall back to the defaults when it composes each
    /// consumer's configuration). The connection is validated lazily — a producer-only service needs
    /// no <c>Bus:Consumer</c> section.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The global consumer configuration.</returns>
    private static ConsumerConfiguration CreateConsumerConfiguration(IConfiguration configuration)
        => configuration.GetSection(CONSUMER_SECTION).Get<ConsumerConfiguration>() ?? new ConsumerConfiguration();

    private static IProducer<Null, byte[]> CreateProducer(IServiceProvider provider, ProducerConfig configuration)
    {
        ILogger<ProducerWorker> logger = provider.GetRequiredService<ILogger<ProducerWorker>>();

        return new ProducerBuilder<Null, byte[]>(configuration)
            .SetErrorHandler((_, error) => ClientLogger.LogError(logger, error))
            .SetLogHandler((_, log) => ClientLogger.Log(logger, log))
            .Build();
    }
}
