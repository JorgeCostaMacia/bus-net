using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using JorgeCostaMacia.Bus.Kafka.Tests.Fakes;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Kafka;

/// <summary>
/// The librdkafka callback logging. These are visibility-only behaviours — the log is the whole effect —
/// so each test pins the level the client's input maps to, which is what decides whether an operator is
/// paged or not.
/// </summary>
public class KafkaLoggerTests
{
    [Fact]
    public void LogError_WhenTheClientIsFatal_LogsCritical()
    {
        // a fatal client cannot recover, and the error handler stops the application over it — Critical
        // is the level that says the host is dying.
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.LogError(logger, new Error(ErrorCode.Local_Fatal, "fatal", isFatal: true));

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Critical, entry.Level);
        Assert.Equal("Kafka error.", entry.Message);
    }

    [Fact]
    public void LogError_WhenTheClientRecoversOnItsOwn_LogsWarning()
    {
        // every non-fatal error is informational: the client reconnects by itself, so this must not
        // read as an outage the operator has to act on.
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.LogError(logger, new Error(ErrorCode.Local_AllBrokersDown, "down", isFatal: false));

        Assert.Equal(LogLevel.Warning, Assert.Single(logger.Logged).Level);
    }

    [Fact]
    public void Log_BelowError_PassesTheSeverityThrough()
    {
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.Log(logger, new LogMessage("client", SyslogLevel.Info, "facility", "internal"));

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal("Kafka log.", entry.Message);
    }

    [Fact]
    public void Log_AtACritFacility_IsCappedAtError()
    {
        // this is the cap that matters: librdkafka's crit/alert/emerg facilities are informational for
        // the host (the client recovers), so they must not arrive as Critical — that level is reserved
        // for a dying host, and only the error handler's fatal path uses it.
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.Log(logger, new LogMessage("client", SyslogLevel.Critical, "facility", "internal"));

        Assert.Equal(LogLevel.Error, Assert.Single(logger.Logged).Level);
    }

    [Fact]
    public void Log_AtTheEmergencyFacility_IsAlsoCappedAtError()
    {
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.Log(logger, new LogMessage("client", SyslogLevel.Emergency, "facility", "internal"));

        Assert.Equal(LogLevel.Error, Assert.Single(logger.Logged).Level);
    }

    [Fact]
    public void Log_AtError_IsLeftAtError()
    {
        // the boundary of the cap: Error is not above Error, so it passes through untouched.
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.Log(logger, new LogMessage("client", SyslogLevel.Error, "facility", "internal"));

        Assert.Equal(LogLevel.Error, Assert.Single(logger.Logged).Level);
    }

    [Fact]
    public void LogStatistics_LogsAtDebug()
    {
        // statistics are opt-in and high volume: Debug keeps them out of a normally-configured log.
        RecordingLogger<KafkaLoggerTests> logger = new RecordingLogger<KafkaLoggerTests>();

        KafkaLogger.LogStatistics(logger, "{}");

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Debug, entry.Level);
        Assert.Equal("Kafka statistics.", entry.Message);
    }

    // the category is public API in practice: it is what an operator puts in appsettings to silence
    // the client's noise without touching the bus's own logs.
    [Fact]
    public void Category_IsTheDedicatedClientCategory()
        => Assert.Equal("JorgeCostaMacia.Bus.Kafka.Client", KafkaLogger.Category);
}
