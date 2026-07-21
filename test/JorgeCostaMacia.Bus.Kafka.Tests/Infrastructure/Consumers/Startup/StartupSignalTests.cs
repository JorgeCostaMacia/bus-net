using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers.Startup;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers.Startup;

public class StartupSignalTests
{
    [Fact]
    public void Ready_BeforeMarkReady_IsNotCompleted()
    {
        StartupSignal signal = new StartupSignal();

        Assert.False(signal.Ready.IsCompleted);
    }

    [Fact]
    public async Task MarkReady_CompletesReady()
    {
        StartupSignal signal = new StartupSignal();

        signal.MarkReady();

        await signal.Ready;

        Assert.True(signal.Ready.IsCompletedSuccessfully);
    }

    [Fact]
    public void MarkReady_CalledTwice_IsIdempotent()
    {
        StartupSignal signal = new StartupSignal();

        signal.MarkReady();
        signal.MarkReady();

        Assert.True(signal.Ready.IsCompletedSuccessfully);
    }
}
