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
        Dictionary<string, object> cyclic = [];
        cyclic["self"] = cyclic;
        exception.Data["cyclic"] = cyclic;
        exception.Data["type"] = typeof(InvalidOperationException);
        exception.Data["order"] = 42;

        ErrorInfo error = ErrorInfo.Create(exception);

        Assert.IsType<string>(error.Data["cyclic"]);
        Assert.Equal(typeof(InvalidOperationException).ToString(), error.Data["type"]);
        Assert.Equal(42, error.Data["order"]);   // serializable values keep their type
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
