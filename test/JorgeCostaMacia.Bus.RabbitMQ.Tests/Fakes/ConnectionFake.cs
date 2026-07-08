using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

/// <summary>In-memory double of the shared connection — hands out the one <see cref="ChannelFake"/> the producer publishes through.</summary>
internal sealed class ConnectionFake : Domain.IConnection
{
    private readonly ChannelFake _channel;

    /// <summary>Creates the fake over the channel it hands out.</summary>
    /// <param name="channel">The channel every <see cref="CreateChannelAsync"/> returns.</param>
    public ConnectionFake(ChannelFake channel) => _channel = channel;

    /// <inheritdoc />
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default) => Task.FromResult<IChannel>(_channel);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
