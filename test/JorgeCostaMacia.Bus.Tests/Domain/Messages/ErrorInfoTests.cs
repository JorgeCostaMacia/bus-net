using System.Text.Json;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.Tests.Domain.Messages;

public class ErrorInfoTests
{
    [Fact]
    public void Create_MapsTypeMessageAndStackTrace()
    {
        Exception thrown = Throw(new InvalidOperationException("boom"));

        ErrorInfo error = ErrorInfo.Create(thrown);

        Assert.Equal("System.InvalidOperationException", error.Type);
        Assert.Equal("boom", error.Message);
        Assert.NotNull(error.StackTrace);
        Assert.Null(error.InnerError);
    }

    [Fact]
    public void Create_RecursesTheInnerExceptionChain()
    {
        InvalidOperationException outer = new("outer", new ArgumentException("middle", new FormatException("deepest")));

        ErrorInfo error = ErrorInfo.Create(outer);

        Assert.Equal("outer", error.Message);
        Assert.Equal("System.ArgumentException", error.InnerError?.Type);
        Assert.Equal("System.FormatException", error.InnerError?.InnerError?.Type);
        Assert.Null(error.InnerError?.InnerError?.InnerError);
    }

    [Fact]
    public void Create_NoData_YieldsEmptyDictionary()
        => Assert.Empty(ErrorInfo.Create(new InvalidOperationException("boom")).Data);

    [Fact]
    public void Create_ExtractsData_StringifyingNonStringKeys()
    {
        InvalidOperationException exception = new("boom");
        exception.Data["order"] = "42";
        exception.Data[7] = "seven";
        exception.Data["nothing"] = null;

        ErrorInfo error = ErrorInfo.Create(exception);

        Assert.Equal("42", error.Data["order"]);
        Assert.Equal("seven", error.Data["7"]);   // non-string keys travel as their ToString
        Assert.Null(error.Data["nothing"]);
        Assert.Equal(3, error.Data.Count);
    }

    [Fact]
    public void Create_UnserializableDataValues_TravelAsText()
    {
        // the parked error is serialized through both failure lanes — a value that cannot serialize
        // (a reference cycle, a Type…) must degrade to its text instead of poisoning the park and
        // turning the failure into a hot redelivery loop.
        InvalidOperationException exception = new("boom");
        Dictionary<string, object> cyclic = new Dictionary<string, object>();
        cyclic["self"] = cyclic;
        exception.Data["cyclic"] = cyclic;
        exception.Data["type"] = typeof(InvalidOperationException);
        exception.Data["order"] = 42;

        ErrorInfo error = ErrorInfo.Create(exception);

        Assert.IsType<string>(error.Data["cyclic"]);
        Assert.Equal(typeof(InvalidOperationException).ToString(), error.Data["type"]);
        Assert.Equal(42, error.Data["order"]);   // serializable values keep their type
    }

    [Fact]
    public void Create_MapsSource()
    {
        Exception thrown = Throw(new InvalidOperationException("boom"));

        ErrorInfo error = ErrorInfo.Create(thrown);

        Assert.NotNull(error.Source);            // the runtime stamps Source when the exception is thrown
        Assert.Equal(thrown.Source, error.Source);
    }

    [Fact]
    public void RoundTrip_PreservesTheParkedContract()
    {
        // the parked failure travels serialized through both failure lanes and tooling reads it back
        // with the Web options — Type/Message/Source/StackTrace and the whole inner-cause chain survive.
        Exception thrown = Throw(new InvalidOperationException("outer", new FormatException("inner")));
        ErrorInfo error = ErrorInfo.Create(thrown);
        JsonSerializerOptions options = new(JsonSerializerDefaults.Web);

        ErrorInfo roundTripped = JsonSerializer.Deserialize<ErrorInfo>(JsonSerializer.Serialize(error, options), options)!;

        Assert.Equal(error.Type, roundTripped.Type);
        Assert.Equal(error.Message, roundTripped.Message);
        Assert.Equal(error.Source, roundTripped.Source);
        Assert.Equal(error.StackTrace, roundTripped.StackTrace);
        Assert.Equal("System.FormatException", roundTripped.InnerError?.Type);
        Assert.Equal("inner", roundTripped.InnerError?.Message);
        Assert.Null(roundTripped.InnerError?.InnerError);
    }

    [Fact]
    public void Create_DataKeyCollision_LastStringifiedWriteWins()
    {
        // an int key and a string key that stringify to the same text collapse to one entry — the
        // later write wins (Exception.Data preserves insertion order).
        InvalidOperationException exception = new("boom");
        exception.Data[7] = "int-seven";
        exception.Data["7"] = "string-seven";

        ErrorInfo error = ErrorInfo.Create(exception);

        Assert.Equal("string-seven", error.Data["7"]);
        Assert.Single(error.Data);
    }

    private static Exception Throw(Exception exception)
    {
        try
        {
            throw exception;
        }
        catch (Exception thrown)
        {
            return thrown;
        }
    }
}
