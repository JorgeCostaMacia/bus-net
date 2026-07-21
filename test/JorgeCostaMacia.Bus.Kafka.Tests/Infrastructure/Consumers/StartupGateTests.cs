using JorgeCostaMacia.Bus.Kafka.Infrastructure.Consumers;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Consumers;

public class StartupGateTests
{
    [Fact]
    public async Task WaitAsync_UpToTheLimit_ProceedsWithoutBlocking()
    {
        StartupGate gate = new StartupGate(2);

        Task first = gate.WaitAsync(CancellationToken.None);
        Task second = gate.WaitAsync(CancellationToken.None);

        await first;
        await second;

        Assert.True(first.IsCompletedSuccessfully);
        Assert.True(second.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitAsync_BeyondTheLimit_BlocksUntilASlotIsReleased()
    {
        StartupGate gate = new StartupGate(1);
        await gate.WaitAsync(CancellationToken.None);

        Task waiter = gate.WaitAsync(CancellationToken.None);

        Assert.False(waiter.IsCompleted);

        gate.Release();

        await waiter;

        Assert.True(waiter.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Constructor_ClampsANonPositiveLimitToOne()
    {
        StartupGate gate = new StartupGate(0);

        await gate.WaitAsync(CancellationToken.None);

        Task waiter = gate.WaitAsync(CancellationToken.None);

        Assert.False(waiter.IsCompleted);

        gate.Release();

        await waiter;

        Assert.True(waiter.IsCompletedSuccessfully);
    }
}
