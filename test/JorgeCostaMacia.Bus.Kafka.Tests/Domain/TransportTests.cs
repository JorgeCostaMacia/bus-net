using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests;

public class TransportTests
{
    private static Transport CreateSut(Headers headers)
        => new(headers.ToImmutableList(), "orders", new Partition(1), new Offset(10), null, new Timestamp(DateTime.UtcNow));

    [Fact]
    public void Create_FromConsumeResult_MapsTheDelivery()
    {
        ConsumeResult<Null, byte[]> result = new()
        {
            TopicPartitionOffset = new TopicPartitionOffset("orders", new Partition(3), new Offset(42)),
            Message = new Message<Null, byte[]> { Value = [], Headers = new Headers { { "key", "value"u8.ToArray() } } }
        };

        Transport transport = Transport.Create(result);

        Assert.Equal("orders", transport.Topic);
        Assert.Equal(3, transport.Partition.Value);
        Assert.Equal(42, transport.Offset.Value);
        Assert.Single(transport.Headers);
    }

    [Fact]
    public void GetGuid_SixteenRawBytes_RoundTrips()
    {
        Guid value = Guid.NewGuid();

        Assert.Equal(value, CreateSut([new Header("id", value.ToByteArray())]).GetGuid("id"));
    }

    [Fact]
    public void GetGuid_WrongLength_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut([new Header("id", [1, 2, 3])]).GetGuid("id"));

    [Fact]
    public void GetHeader_Missing_Throws()
        => Assert.Throws<KeyNotFoundException>(() => CreateSut([]).GetString("missing"));

    [Fact]
    public void GetHeader_DuplicateKey_LastWins()
        => Assert.Equal("second", CreateSut([new Header("key", "first"u8.ToArray()), new Header("key", "second"u8.ToArray())]).GetString("key"));

    [Fact]
    public void GetStringOrDefault_Absent_ReturnsNull()
    {
        Transport transport = CreateSut([new Header("present", "value"u8.ToArray())]);

        Assert.Equal("value", transport.GetStringOrDefault("present"));
        Assert.Null(transport.GetStringOrDefault("absent"));
    }

    [Fact]
    public void GetDateTime_RoundTripFormat_ParsesAsUtc()
    {
        DateTime value = new(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

        DateTime parsed = CreateSut([new Header("at", Encoding.UTF8.GetBytes(value.ToString("O")))]).GetDateTime("at");

        Assert.Equal(value, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
    }

    [Fact]
    public void GetDateTime_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut([new Header("at", "nope"u8.ToArray())]).GetDateTime("at"));

    [Fact]
    public void GetStringList_TrimsAndSkipsEmptyEntries()
        => Assert.Equal(["a", "b", "c"], CreateSut([new Header("list", " a, b ,,c "u8.ToArray())]).GetStringList("list"));

    [Fact]
    public void GetInt_Digits_Parses()
        => Assert.Equal(7, CreateSut([new Header("count", "7"u8.ToArray())]).GetInt("count"));

    [Fact]
    public void GetInt_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut([new Header("count", "nope"u8.ToArray())]).GetInt("count"));

    [Fact]
    public void CloneHeaders_DeepCopiesTheValues()
    {
        byte[] original = "value"u8.ToArray();
        Transport transport = CreateSut([new Header("key", original)]);

        Headers cloned = transport.CloneHeaders();
        cloned.TryGetLastBytes("key", out byte[] copy);
        copy[0] = 0;

        Assert.Equal("value"u8.ToArray(), original);
        Assert.Equal("value", transport.GetString("key"));
    }
}
