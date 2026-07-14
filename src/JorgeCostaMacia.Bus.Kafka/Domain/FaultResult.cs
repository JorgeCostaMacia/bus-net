namespace JorgeCostaMacia.Bus.Kafka.Domain;

/// <summary>
/// How a fault handler left a broken delivery — read by the consumer (the orchestrator) after it
/// invokes the handler, since the handler's <c>Handle</c> returns <see cref="System.Threading.Tasks.Task"/>
/// by contract and cannot report it. The consumer acks the delivery on <see cref="Parked"/> and
/// leaves it unacked on <see cref="Unhandled"/>.
/// </summary>
internal enum FaultResult
{
    /// <summary>The handler could not park the delivery (a failed produce) — nothing holds it: the worker logs it critical with its coordinates and the next commit on the partition buries it (recover by restarting before that, or re-injecting from the topic).</summary>
    Unhandled,

    /// <summary>Parked to the fault topic.</summary>
    Parked
}
