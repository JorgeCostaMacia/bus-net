using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Serilog;
using Serilog.Events;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure;

/// <summary>
/// The bus's own logging: which events are worth a log at all, and the context properties that make a
/// failure queryable afterwards. The header decoding is the substance here — a correlation id logged as
/// a <see cref="Guid"/> instead of a base64 blob is what lets an operator find the delivery — so the
/// properties are read back through a real Serilog pipeline rather than assumed.
/// </summary>
public class BusLoggerTests
{
    private readonly CapturingSink _sink = new CapturingSink();

    [Fact]
    public void LogCommit_WhenEveryOffsetCommitted_LogsNothing()
    {
        // the happy path is the common one and it is silent on purpose: logging every successful
        // background commit would bury the failures that actually widen the redelivery window.
        RecordingLogger<BusLoggerTests> logger = new RecordingLogger<BusLoggerTests>();

        BusLogger.LogCommit(logger, new CommittedOffsets(new List<TopicPartitionOffsetError>(), new Error(ErrorCode.NoError)));

        Assert.Empty(logger.Logged);
    }

    [Fact]
    public void LogCommit_WhenTheCommitItselfFailed_Warns()
    {
        RecordingLogger<BusLoggerTests> logger = new RecordingLogger<BusLoggerTests>();

        BusLogger.LogCommit(logger, new CommittedOffsets(new List<TopicPartitionOffsetError>(), new Error(ErrorCode.Local_TimedOut, "timeout")));

        Assert.Equal("Commit failed.", Assert.Single(logger.Logged).Message);
    }

    [Fact]
    public void LogCommit_WhenASingleOffsetFailed_StillWarns()
    {
        // the per-offset errors are the ones easiest to miss: the call reports NoError overall while an
        // individual partition's offset did not store, so its messages will be redelivered.
        RecordingLogger<BusLoggerTests> logger = new RecordingLogger<BusLoggerTests>();
        List<TopicPartitionOffsetError> offsets = new List<TopicPartitionOffsetError>()
        {
            new TopicPartitionOffsetError("orders", 0, 10, new Error(ErrorCode.NoError)),
            new TopicPartitionOffsetError("orders", 1, 20, new Error(ErrorCode.Local_Fail, "no store"))
        };

        BusLogger.LogCommit(logger, new CommittedOffsets(offsets, new Error(ErrorCode.NoError)));

        Assert.Equal("Commit failed.", Assert.Single(logger.Logged).Message);
    }

    [Fact]
    public void LogPartitionsLost_Warns()
    {
        // falling out of the group is nothing the worker can act on, but the partitions' new owners will
        // redeliver from the last committed offset — worth a warning, not silence.
        RecordingLogger<BusLoggerTests> logger = new RecordingLogger<BusLoggerTests>();
        List<TopicPartitionOffset> partitions = new List<TopicPartitionOffset>()
        {
            new TopicPartitionOffset("orders", 0, 10)
        };

        BusLogger.LogPartitionsLost(logger, partitions);

        Assert.Equal("Partitions lost.", Assert.Single(logger.Logged).Message);
    }

    [Fact]
    public void ProducerContext_DecodesTheTransportHeadersToTheirRealTypes()
    {
        // a Guid header must read as a Guid and a counter as a number: that is what makes them
        // filterable in the log platform instead of opaque text.
        Guid messageId = Guid.CreateVersion7();
        Headers headers = new Headers();
        headers.Add(TransportHeaders.MessageId, messageId.ToByteArray());
        headers.Add(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes("3"));
        headers.Add("jcm-plain", Encoding.UTF8.GetBytes("text"));

        LogEvent captured = Capture(() => BusLogger.ProducerContext("orders", Message(headers)));

        Assert.Equal(messageId, CapturingSink.Scalar(captured, TransportHeaders.MessageId));
        Assert.Equal(3, CapturingSink.Scalar(captured, TransportHeaders.RetryCount));
        Assert.Equal("text", CapturingSink.Scalar(captured, "jcm-plain"));
    }

    [Fact]
    public void ProducerContext_WithAGuidHeaderOfTheWrongLength_FallsBackToText()
    {
        // the length check is what stops a malformed id throwing inside the logging of a failure, which
        // would replace the real error with this one.
        Headers headers = new Headers();
        headers.Add(TransportHeaders.MessageId, Encoding.UTF8.GetBytes("not-a-guid"));

        LogEvent captured = Capture(() => BusLogger.ProducerContext("orders", Message(headers)));

        Assert.Equal("not-a-guid", CapturingSink.Scalar(captured, TransportHeaders.MessageId));
    }

    [Fact]
    public void ProducerContext_WithAnUnparseableCountHeader_FallsBackToText()
    {
        Headers headers = new Headers();
        headers.Add(TransportHeaders.RetryCount, Encoding.UTF8.GetBytes("many"));

        LogEvent captured = Capture(() => BusLogger.ProducerContext("orders", Message(headers)));

        Assert.Equal("many", CapturingSink.Scalar(captured, TransportHeaders.RetryCount));
    }

    [Fact]
    public void ProducerContext_WithAValuelessHeader_DecodesItAsNull()
    {
        Headers headers = new Headers();
        headers.Add(new Header("jcm-empty", null));

        LogEvent captured = Capture(() => BusLogger.ProducerContext("orders", Message(headers)));

        Assert.Null(CapturingSink.Scalar(captured, "jcm-empty"));
    }

    [Fact]
    public void ProducerContext_WithNoBodyAndNoHeaders_StillCarriesTheTopic()
    {
        // this runs inside the produce failure handler: if building the context threw, the exception the
        // operator sees would be this one instead of the produce failure that caused it. The nulls are
        // forgiven deliberately — Message declares both non-nullable, but the logger guards them because
        // a message off the wire can carry neither, and that guard is what is under test.
        LogEvent captured = Capture(() => BusLogger.ProducerContext("orders", new Message<Null, byte[]>() { Value = null!, Headers = null! }));

        Assert.Equal("orders", CapturingSink.Scalar(captured, "Topic"));
        Assert.Null(CapturingSink.Scalar(captured, "Body"));
    }

    /// <summary>Builds an outbound message with a fixed body and the given headers.</summary>
    /// <param name="headers">The headers.</param>
    /// <returns>The message.</returns>
    private static Message<Null, byte[]> Message(Headers headers)
        => new Message<Null, byte[]>() { Value = Encoding.UTF8.GetBytes("body"), Headers = headers };

    /// <summary>
    /// Runs a context factory inside a real Serilog pipeline and returns the single event written while
    /// the context was open, so the pushed properties can be read back.
    /// </summary>
    /// <param name="context">The context factory under test.</param>
    /// <returns>The captured event.</returns>
    private LogEvent Capture(Func<IDisposable> context)
    {
        Log.Logger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Sink(_sink)
            .CreateLogger();

        try
        {
            using (context())
            {
                Log.Information("probe");
            }
        }
        finally
        {
            Log.CloseAndFlush();
        }

        return Assert.Single(_sink.Events);
    }
}
