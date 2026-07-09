namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// How an error handler left a failed delivery — read by the consumer (the orchestrator) after it
/// invokes the handler, since the handler's <c>Handle</c> returns <see cref="System.Threading.Tasks.Task"/>
/// by contract and cannot report it. The consumer acks the delivery on every outcome except
/// <see cref="Unhandled"/>, and hands it to the fault handler on <see cref="Faulted"/>. The fault
/// handler reports its own <see cref="FaultResult"/>.
/// </summary>
internal enum ErrorResult
{
    /// <summary>The handler could not cope (a failed produce, a transient fault) — the worker escalates the delivery to the fault handler: an unmanageable failure belongs to the fault lane.</summary>
    Unhandled,

    /// <summary>Requeued to the topic's tail for an immediate retry.</summary>
    Retried,

    /// <summary>Parked through the retry scheduler for a delayed retry.</summary>
    Scheduled,

    /// <summary>Parked to the error topic.</summary>
    Parked,

    /// <summary>The delivery's envelope or body is unreadable — the fault handler takes the relay.</summary>
    Faulted
}
