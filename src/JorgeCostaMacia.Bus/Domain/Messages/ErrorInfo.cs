using System.Collections;
using System.Collections.Immutable;
using System.Text.Json;

namespace JorgeCostaMacia.Bus.Domain.Messages;

/// <summary>
/// A serializable, recursive model of a failure — the exception's type, message, source and stack
/// trace, plus its inner exception as another <see cref="ErrorInfo"/>, so the whole cause chain is
/// captured (a raw <see cref="Exception"/> does not serialize cleanly). Carried by an
/// <see cref="IErrorMessage"/> so tooling can read <b>and</b> query a parked failure — filter by the
/// inner cause's type, not just read a flattened string.
/// </summary>
public sealed record ErrorInfo
{
    /// <summary>Full type name of the exception.</summary>
    public string Type { get; init; }

    /// <summary>The exception's message.</summary>
    public string Message { get; init; }

    /// <summary>The exception's source (the application or object that caused it), when available.</summary>
    public string? Source { get; init; }

    /// <summary>The exception's stack trace, when available.</summary>
    public string? StackTrace { get; init; }

    /// <summary>The inner exception as its own <see cref="ErrorInfo"/>, or <see langword="null"/> when there is none — the cause chain, all the way down.</summary>
    public ErrorInfo? InnerError { get; init; }

    /// <summary>
    /// The exception's <see cref="Exception.Data"/> — arbitrary key/value diagnostics the thrower
    /// attached (validation failures, correlation ids, …), each value serialized with its structure.
    /// Empty when the exception carried none.
    /// </summary>
    public ImmutableDictionary<string, object?> Data { get; init; }

    /// <summary>Creates the model over the exception's details, its attached data and its inner cause.</summary>
    /// <param name="type">Full type name of the exception.</param>
    /// <param name="message">The exception's message.</param>
    /// <param name="source">The exception's source, when available.</param>
    /// <param name="stackTrace">The exception's stack trace, when available.</param>
    /// <param name="data">The exception's attached key/value diagnostics.</param>
    /// <param name="innerError">The inner exception as its own model, or <see langword="null"/>.</param>
    public ErrorInfo(string type, string message, string? source, string? stackTrace, ImmutableDictionary<string, object?> data, ErrorInfo? innerError)
    {
        Type = type;
        Message = message;
        Source = source;
        StackTrace = stackTrace;
        Data = data;
        InnerError = innerError;
    }

    /// <summary>Models an exception and its whole inner-exception chain recursively.</summary>
    /// <param name="exception">The exception to model.</param>
    /// <returns>The serializable model of the exception and its cause chain.</returns>
    public static ErrorInfo Create(Exception exception)
    {
        Type type = exception.GetType();

        return new(
            type.FullName ?? type.Name,
            exception.Message,
            exception.Source,
            exception.StackTrace,
            ExtractData(exception),
            exception.InnerException is null ? null : Create(exception.InnerException));
    }

    /// <summary>Captures the exception's <see cref="Exception.Data"/> as a serializable dictionary — keys as text, the last wins on a collision, values sanitized through <see cref="Sanitize"/>; empty when there is none.</summary>
    private static ImmutableDictionary<string, object?> ExtractData(Exception exception)
    {
        if (exception.Data.Count == 0) return ImmutableDictionary<string, object?>.Empty;

        ImmutableDictionary<string, object?>.Builder data = ImmutableDictionary.CreateBuilder<string, object?>();

        foreach (DictionaryEntry entry in exception.Data)
        {
            data[entry.Key.ToString()!] = Sanitize(entry.Value);
        }

        return data.ToImmutable();
    }

    /// <summary>
    /// Keeps a value only if it survives JSON serialization; anything else (a reference cycle, a
    /// <see cref="Type"/>, a delegate…) is captured as its text. The parked error travels serialized
    /// through both failure lanes — a value that cannot serialize would poison them and turn the
    /// failure into a hot redelivery loop.
    /// </summary>
    private static object? Sanitize(object? value)
    {
        if (value is null) return null;

        try
        {
            JsonSerializer.Serialize(value);

            return value;
        }
        catch (Exception)
        {
            return value.ToString();
        }
    }
}
