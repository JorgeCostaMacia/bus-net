namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A message describing a failure — the contract of the bodies a transport parks when a delivery
/// fails terminally (handler failures, broken deliveries), so tooling can read any parked failure
/// the same way regardless of the transport or the failure kind.
/// </summary>
public interface IErrorMessage : IMessage
{
    /// <summary>The failure, modeled with its whole inner-exception chain — type, message, source, stack trace and inner error.</summary>
    ErrorInfo Error { get; }

    /// <summary>UTC time the failure was parked.</summary>
    DateTime ErrorOccurredAt { get; }
}
