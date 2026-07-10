using JorgeCostaMacia.Bus.Kafka.Domain.Commands.Faults;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>
/// Fault handler double that triggers the worker's stop mid-delivery and then observes the loop's
/// token as a real produce would — drives the shutdown-cancellation lane (an
/// <see cref="OperationCanceledException"/> in the fault lane is rethrown for the loop to exit
/// through, the delivery left unacked).
/// </summary>
internal sealed class StoppingCommandFaultHandler : CommandFaultHandler<TestCommand, RecordingCommandHandler>
{
    private readonly TaskCompletionSource<Task> _stopping = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Initiates the worker's stop; its task completes when the stop does.</summary>
    public Func<Task>? Stop { get; set; }

    /// <summary>Completes with the in-flight stop task once the handler triggered it.</summary>
    public Task<Task> Stopping => _stopping.Task;

    public override Task Handle(CommandFaultContext context, CancellationToken cancellationToken = default)
    {
        _stopping.TrySetResult(Stop?.Invoke() ?? Task.CompletedTask);

        cancellationToken.ThrowIfCancellationRequested();

        return Task.CompletedTask;
    }
}
