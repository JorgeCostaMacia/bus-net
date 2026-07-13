using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Fakes;

/// <summary>Application lifetime double — records whether the consumer asked to stop the application on a fatal client error.</summary>
internal sealed class LifetimeFake : IHostApplicationLifetime
{
    private readonly CancellationTokenSource _stopping = new CancellationTokenSource();

    /// <summary>Whether <see cref="StopApplication"/> was called.</summary>
    public bool StopRequested { get; private set; }

    public CancellationToken ApplicationStarted => CancellationToken.None;

    public CancellationToken ApplicationStopping => _stopping.Token;

    public CancellationToken ApplicationStopped => CancellationToken.None;

    public void StopApplication()
    {
        StopRequested = true;
        _stopping.Cancel();
    }
}
