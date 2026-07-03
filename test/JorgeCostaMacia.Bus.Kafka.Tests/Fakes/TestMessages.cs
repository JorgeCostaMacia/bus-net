namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>Command used across the tests, forwarding the traceability metadata to the base.</summary>
internal sealed record TestCommand : Domain.Command
{
    public string Name { get; init; }

    public TestCommand(string name, Guid? aggregateId = null, Guid? aggregateCorrelationId = null, DateTime? aggregateOccurredAt = null, IEnumerable<string>? aggregateConsumers = null)
        : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
        => Name = name;
}

/// <summary>Event used across the tests, forwarding the traceability metadata to the base.</summary>
internal sealed record TestEvent : Domain.Event
{
    public string Name { get; init; }

    public TestEvent(string name, Guid? aggregateId = null, Guid? aggregateCorrelationId = null, DateTime? aggregateOccurredAt = null, IEnumerable<string>? aggregateConsumers = null)
        : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
        => Name = name;
}

/// <summary>No-op handler used to exercise the configurator registrations.</summary>
internal sealed class TestCommandHandler : Domain.CommandHandler<TestCommand>
{
    public override Task Handle(Domain.CommandContext<TestCommand> context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

/// <summary>No-op subscriber used to exercise the configurator registrations.</summary>
internal sealed class TestEventSubscriber : Domain.EventSubscriber<TestEvent>
{
    public override Task Handle(Domain.EventContext<TestEvent> context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
