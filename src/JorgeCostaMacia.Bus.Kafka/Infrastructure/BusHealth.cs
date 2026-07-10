namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The bus's broker-reachability tracker — a container-owned singleton fed by the transport itself:
/// the librdkafka error callbacks flip it down on <c>AllBrokersDown</c> (every producer and consumer
/// connection lost), and any successful produce or consumed delivery flips it back up. It starts up —
/// nothing observed yet means the brokers are assumed reachable — and stamps the UTC instant of each
/// flip, so a health check can report both the state and how long it has held. Thread-safe: the
/// callbacks, the producers and the consumer loops all feed it concurrently, so the flip and its
/// timestamp move together under a lock.
/// </summary>
internal sealed class BusHealth
{
    private readonly object _gate = new();

    private bool _isUp = true;
    private DateTime _changedAt = DateTime.UtcNow;

    /// <summary>Whether the brokers are reachable — up until the client reports every broker down, up again on the first successful produce or consumed delivery.</summary>
    public bool IsUp
    {
        get
        {
            lock (_gate)
            {
                return _isUp;
            }
        }
    }

    /// <summary>The UTC instant of the last state flip — construction time while nothing has flipped it yet.</summary>
    public DateTime ChangedAt
    {
        get
        {
            lock (_gate)
            {
                return _changedAt;
            }
        }
    }

    /// <summary>Reports the brokers reachable — a successful produce or consumed delivery; stamps <see cref="ChangedAt"/> only on an actual flip.</summary>
    public void Up() => Set(up: true);

    /// <summary>Reports every broker unreachable — the client's <c>AllBrokersDown</c>; stamps <see cref="ChangedAt"/> only on an actual flip.</summary>
    public void Down() => Set(up: false);

    /// <summary>Flips the state under the gate — only on an actual change, so <see cref="ChangedAt"/> keeps the instant of the flip, not of the last report.</summary>
    private void Set(bool up)
    {
        lock (_gate)
        {
            if (_isUp == up) return;

            _isUp = up;
            _changedAt = DateTime.UtcNow;
        }
    }
}
