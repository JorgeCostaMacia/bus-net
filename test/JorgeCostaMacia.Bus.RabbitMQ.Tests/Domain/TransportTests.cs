using System.Collections.Immutable;
using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Fakes;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests;

public class TransportTests
{
    private static Transport CreateSut(params (string Key, byte[] Value)[] headers)
        => new(headers.ToDictionary(header => header.Key, header => (object?)header.Value), "orders", string.Empty, deliveryTag: 10, redelivered: false);

    [Fact]
    public void Create_FromDeliveryArgs_MapsTheDelivery()
    {
        Transport transport = Transport.Create(Deliveries.Args("{}"u8.ToArray(), new Dictionary<string, object?> { ["key"] = "value"u8.ToArray() }, deliveryTag: 42));

        Assert.Equal(Deliveries.EXCHANGE, transport.Exchange);
        Assert.Equal(string.Empty, transport.RoutingKey);
        Assert.Equal(42ul, transport.DeliveryTag);
        Assert.False(transport.Redelivered);
        Assert.Single(transport.Headers);
    }

    [Fact]
    public void Create_WithoutHeaders_YieldsAnEmptyTable()
    {
        Transport transport = Transport.Create(Deliveries.Args("{}"u8.ToArray(), []));

        Assert.Empty(transport.Headers);
    }

    [Fact]
    public void GetGuid_SixteenRawBytes_RoundTrips()
    {
        Guid value = Guid.NewGuid();

        Assert.Equal(value, CreateSut(("id", value.ToByteArray())).GetHeaderGuid("id"));
    }

    [Fact]
    public void GetGuid_WrongLength_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("id", [1, 2, 3])).GetHeaderGuid("id"));

    [Fact]
    public void GetHeader_Missing_Throws()
        => Assert.Throws<KeyNotFoundException>(() => CreateSut().GetHeaderString("missing"));

    [Fact]
    public void GetStringOrDefault_Absent_ReturnsNull()
    {
        Transport transport = CreateSut(("present", "value"u8.ToArray()));

        Assert.Equal("value", transport.GetHeaderStringOrDefault("present"));
        Assert.Null(transport.GetHeaderStringOrDefault("absent"));
    }

    [Fact]
    public void GetDateTime_RoundTripFormat_ParsesAsUtc()
    {
        DateTime value = new(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

        DateTime parsed = CreateSut(("at", Encoding.UTF8.GetBytes(value.ToString("O")))).GetHeaderDateTime("at");

        Assert.Equal(value, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
    }

    [Fact]
    public void GetDateTime_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("at", "nope"u8.ToArray())).GetHeaderDateTime("at"));

    [Fact]
    public void GetStringList_TrimsAndSkipsEmptyEntries()
        => Assert.Equal(["a", "b", "c"], CreateSut(("list", " a, b ,,c "u8.ToArray())).GetHeaderStringList("list"));

    [Fact]
    public void GetInt_Digits_Parses()
        => Assert.Equal(7, CreateSut(("count", "7"u8.ToArray())).GetHeaderInt("count"));

    [Fact]
    public void GetInt_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("count", "nope"u8.ToArray())).GetHeaderInt("count"));

    [Fact]
    public void DecodeHeaders_RendersGuidsAsGuid_TextOtherwise_PreservingOrder()
    {
        Guid id = Guid.NewGuid();
        Transport transport = CreateSut(
            (TransportHeaders.AggregateId, id.ToByteArray()),
            ("custom", "value"u8.ToArray()));

        ImmutableList<KeyValuePair<string, string>> decoded = transport.DecodeHeaders();

        Assert.Equal(2, decoded.Count);
        Assert.Contains(new KeyValuePair<string, string>(TransportHeaders.AggregateId, id.ToString()), decoded);
        Assert.Contains(new KeyValuePair<string, string>("custom", "value"), decoded);
    }

    [Fact]
    public void CloneHeaders_DeepCopiesTheValues()
    {
        byte[] original = "value"u8.ToArray();
        Transport transport = CreateSut(("key", original));

        Dictionary<string, object?> cloned = transport.CloneHeaders();
        ((byte[])cloned["key"]!)[0] = 0;

        Assert.Equal("value"u8.ToArray(), original);
        Assert.Equal("value", transport.GetHeaderString("key"));
    }
}
