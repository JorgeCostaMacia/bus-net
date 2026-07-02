using System.Collections.Immutable;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Event.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// Registers the bus's messages and handlers, carrying the connection from the global
/// <see cref="BusConfiguration"/> so it is declared once and never repeated. Each context maps its
/// own pieces through it (<c>Map*BusContext(this BusConfigurator)</c> extensions): messages it sends
/// (<see cref="AddCommand{TCommand}"/> / <see cref="AddEvent{TEvent}"/>) and messages it consumes
/// (<see cref="AddCommandHandler{TCommand, TCommandHandler}"/> /
/// <see cref="AddEventSubscriber{TEvent, TEventSubscriber}"/> — each hosting its own consumer).
/// </summary>
public sealed class BusConfigurator
{
    private readonly IServiceCollection _services;
    private readonly string _bootstrapServers;
    private readonly string? _saslUsername;
    private readonly string? _saslPassword;
    private readonly SecurityProtocol? _securityProtocol;
    private readonly SaslMechanism? _saslMechanism;

    internal BusConfigurator(IServiceCollection services, BusConfiguration configuration)
    {
        ProducerConfig connection = configuration.ProducerConfig;

        _services = services;
        _bootstrapServers = connection.BootstrapServers;
        _saslUsername = connection.SaslUsername;
        _saslPassword = connection.SaslPassword;
        _securityProtocol = connection.SecurityProtocol;
        _saslMechanism = connection.SaslMechanism;
    }

    /// <summary>Registers a command this service sends, mapping its type to a topic.</summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="topic">The Kafka topic the command is sent to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusConfigurator AddCommand<TCommand>(string topic)
        where TCommand : ICommand
    {
        _services.AddSingleton<IMessageConfiguration>(new CommandConfiguration<TCommand>(topic));

        return this;
    }

    /// <summary>Registers an event this service publishes, mapping its type to a topic.</summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="topic">The Kafka topic the event is published to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusConfigurator AddEvent<TEvent>(string topic)
        where TEvent : IEvent
    {
        _services.AddSingleton<IMessageConfiguration>(new EventConfiguration<TEvent>(topic));

        return this;
    }

    /// <summary>
    /// Registers a command handler: the handler itself (scoped, one per delivery) and its hosted
    /// consumer, configured over the shared connection.
    /// </summary>
    /// <typeparam name="TCommand">The command type consumed.</typeparam>
    /// <typeparam name="TCommandHandler">The handler type.</typeparam>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{topic}.handler</c>) — a stable contract, it holds the group's offsets.</param>
    /// <param name="retryAttempts">Maximum retry requeues to the topic, or <see langword="null"/> for the default (no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retries, or <see langword="null"/> for none.</param>
    /// <param name="redeliveryAttempts">Redelivery attempts, or <see langword="null"/> for the default.</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exceptions excluded from redelivery, or <see langword="null"/> for none.</param>
    /// <param name="autoCommitIntervalMs">Interval (ms) between background commits of the stored offsets, or <see langword="null"/> for the default.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusConfigurator AddCommandHandler<TCommand, TCommandHandler>(
        string topic,
        string groupId,
        int? retryAttempts = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        int? redeliveryAttempts = null,
        ImmutableList<Type>? redeliveryExcludeExceptionTypes = null,
        int? autoCommitIntervalMs = null)
        where TCommand : Domain.Command
        where TCommandHandler : class, ICommandHandler<TCommand, CommandContext<TCommand>, Transport>
    {
        CommandHandlerConfiguration<TCommand, TCommandHandler> configuration = new(
            topic,
            groupId,
            _bootstrapServers,
            _saslUsername,
            _saslPassword,
            retryAttempts,
            retryExcludeExceptionTypes,
            redeliveryAttempts,
            redeliveryExcludeExceptionTypes,
            _securityProtocol,
            _saslMechanism,
            autoCommitIntervalMs: autoCommitIntervalMs);

        _services.AddScoped<TCommandHandler>();
        _services.AddSingleton<IHostedService>(provider => new CommandConsumer<TCommand, TCommandHandler>(
            configuration,
            provider.GetRequiredService<IProducer<Null, byte[]>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<CommandConsumer<TCommand, TCommandHandler>>>()));

        return this;
    }

    /// <summary>
    /// Registers an event subscriber: the subscriber itself (scoped, one per delivery) and its hosted
    /// consumer, configured over the shared connection.
    /// </summary>
    /// <typeparam name="TEvent">The event type consumed.</typeparam>
    /// <typeparam name="TEventSubscriber">The subscriber type.</typeparam>
    /// <param name="topic">The Kafka topic to consume from.</param>
    /// <param name="groupId">The consumer group id (e.g. <c>{consumer}.on.{topic}.subscriber</c>) — a stable contract, unique per subscriber, it holds the group's offsets.</param>
    /// <param name="retryAttempts">Maximum retry requeues to the topic, or <see langword="null"/> for the default (no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exceptions excluded from retries, or <see langword="null"/> for none.</param>
    /// <param name="redeliveryAttempts">Redelivery attempts, or <see langword="null"/> for the default.</param>
    /// <param name="redeliveryExcludeExceptionTypes">Exceptions excluded from redelivery, or <see langword="null"/> for none.</param>
    /// <param name="autoCommitIntervalMs">Interval (ms) between background commits of the stored offsets, or <see langword="null"/> for the default.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public BusConfigurator AddEventSubscriber<TEvent, TEventSubscriber>(
        string topic,
        string groupId,
        int? retryAttempts = null,
        ImmutableList<Type>? retryExcludeExceptionTypes = null,
        int? redeliveryAttempts = null,
        ImmutableList<Type>? redeliveryExcludeExceptionTypes = null,
        int? autoCommitIntervalMs = null)
        where TEvent : Domain.Event
        where TEventSubscriber : class, IEventSubscriber<TEvent, EventContext<TEvent>, Transport>
    {
        EventSubscriberConfiguration<TEvent, TEventSubscriber> configuration = new(
            topic,
            groupId,
            _bootstrapServers,
            _saslUsername,
            _saslPassword,
            retryAttempts,
            retryExcludeExceptionTypes,
            redeliveryAttempts,
            redeliveryExcludeExceptionTypes,
            _securityProtocol,
            _saslMechanism,
            autoCommitIntervalMs: autoCommitIntervalMs);

        _services.AddScoped<TEventSubscriber>();
        _services.AddSingleton<IHostedService>(provider => new EventConsumer<TEvent, TEventSubscriber>(
            configuration,
            provider.GetRequiredService<IProducer<Null, byte[]>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<EventConsumer<TEvent, TEventSubscriber>>>()));

        return this;
    }
}
