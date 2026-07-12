using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>Fault handler double that always throws — drives the worker's last-resort catch (a fault handler that itself fails must leave the delivery unacked without tearing down the loop).</summary>
internal sealed class ThrowingCommandFaultHandler : CommandFaultHandlerBase<TestCommand, RecordingCommandHandler>
{
    public override Task Handle(CommandFaultContext context, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("fault handler down");
}
