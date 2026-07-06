using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using IBus = JorgeCostaMacia.Bus.RabbitMQ.Domain.IBus;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The package's wiring: the single RabbitMQ connection is a container-owned singleton built from the
/// <c>Bus:Connection</c> section; the outbound gate (<see cref="IProducer"/>) and the bus
/// (<see cref="IBus"/>) are <b>scoped</b> — a channel is not concurrency-safe, so each scope gets its
/// own producer over its own channel. The send side maps messages to exchanges through the
/// <see cref="ProducerConfigurator"/>; the optional consume side registers its handlers and their
/// hosted consumers through the <see cref="ConsumerConfigurator"/> (omit it for a send-only service).
/// </summary>
internal static class BusInfrastructureContext
{
    private const string CONNECTION_SECTION = "Bus:Connection";

    /// <summary>Registers the bus over the application configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Connection</c> section.</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their exchanges.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers, or <see langword="null"/> for a send-only service.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddBusInfrastructureContext(this IServiceCollection services, IConfiguration configuration, Action<ProducerConfigurator> producer, Action<ConsumerConfigurator>? consumer = null)
    {
        RabbitConfiguration rabbit = CreateRabbitConfiguration(configuration);

        ProducerConfigurator producerConfigurator = new();

        producer(producerConfigurator);

        services.AddSingleton<IConnection>(_ => new Connection(rabbit.ConnectionFactory));
        services.AddScoped<IProducer, Producer>();
        services.AddScoped<IBus>(provider => new Bus(provider.GetRequiredService<IProducer>(), producerConfigurator.Messages));

        if (consumer is not null)
        {
            ConsumerConfigurator consumerConfigurator = new(services, producerConfigurator.Messages);

            consumer(consumerConfigurator);
        }

        return services;
    }

    /// <summary>Binds and validates the <c>Bus:Connection</c> section onto a <see cref="RabbitConfiguration"/>.</summary>
    private static RabbitConfiguration CreateRabbitConfiguration(IConfiguration configuration)
    {
        RabbitConfiguration rabbit = configuration.GetSection(CONNECTION_SECTION).Get<RabbitConfiguration>()
            ?? throw new InvalidOperationException($"'{CONNECTION_SECTION}' is null.");

        if (string.IsNullOrWhiteSpace(rabbit.HostName)) throw new InvalidOperationException($"'{CONNECTION_SECTION}:{nameof(rabbit.HostName)}' is null.");
        if (string.IsNullOrWhiteSpace(rabbit.UserName)) throw new InvalidOperationException($"'{CONNECTION_SECTION}:{nameof(rabbit.UserName)}' is null.");
        if (string.IsNullOrWhiteSpace(rabbit.Password)) throw new InvalidOperationException($"'{CONNECTION_SECTION}:{nameof(rabbit.Password)}' is null.");

        return rabbit;
    }
}
