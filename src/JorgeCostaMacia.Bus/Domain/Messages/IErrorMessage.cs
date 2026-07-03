namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A message describing a failure — the contract of the bodies a transport parks when a delivery
/// fails terminally (handler failures, broken deliveries), so tooling can read any parked failure
/// the same way regardless of the transport or the failure kind.
/// </summary>
public interface IErrorMessage : IMessage
{
    /// <summary>Full type name of the failure.</summary>
    string ErrorType { get; }

    /// <summary>The failure's message.</summary>
    string ErrorMessage { get; }

    /// <summary>The failure's stack trace, when available.</summary>
    string? ErrorStackTrace { get; }
}
