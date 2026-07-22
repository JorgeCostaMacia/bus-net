using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Domain;

public class TransportTests
{
    private static Transport CreateSut(Headers headers)
        => new Transport(headers.ToImmutableList(), "orders", new Partition(1), new Offset(10), null, new Timestamp(DateTime.UtcNow));

    [Fact]
    public void Create_FromConsumeResult_MapsTheDelivery()
    {
        ConsumeResult<Ignore, byte[]> result = new ConsumeResult<Ignore, byte[]>()
        {
            TopicPartitionOffset = new TopicPartitionOffset("orders", new Partition(3), new Offset(42)),
            Message = new Message<Ignore, byte[]> { Value = Array.Empty<byte>(), Headers = new Headers { { "key", "value"u8.ToArray() } } }
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

        Assert.Equal(value, CreateSut(new Headers { new Header("id", value.ToByteArray()) }).GetHeaderGuid("id"));
    }

    [Fact]
    public void GetGuid_WrongLength_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(new Headers { new Header("id", new byte[] { 1, 2, 3 }) }).GetHeaderGuid("id"));

    [Fact]
    public void GetHeader_Missing_Throws()
        => Assert.Throws<KeyNotFoundException>(() => CreateSut(new Headers()).GetHeaderString("missing"));

    [Fact]
    public void GetHeader_DuplicateKey_LastWins()
        => Assert.Equal("second", CreateSut(new Headers { new Header("key", "first"u8.ToArray()), new Header("key", "second"u8.ToArray()) }).GetHeaderString("key"));

    [Fact]
    public void GetStringOrDefault_Absent_ReturnsNull()
    {
        Transport transport = CreateSut(new Headers { new Header("present", "value"u8.ToArray()) });

        Assert.Equal("value", transport.GetHeaderStringOrDefault("present"));
        Assert.Null(transport.GetHeaderStringOrDefault("absent"));
    }

    [Fact]
    public void GetDateTime_RoundTripFormat_ParsesAsUtc()
    {
        DateTime value = new DateTime(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

        DateTime parsed = CreateSut(new Headers { new Header("at", Encoding.UTF8.GetBytes(value.ToString("O"))) }).GetHeaderDateTime("at");

        Assert.Equal(value, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
    }

    [Fact]
    public void GetDateTime_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(new Headers { new Header("at", "nope"u8.ToArray()) }).GetHeaderDateTime("at"));

    [Fact]
    public void GetStringList_TrimsAndSkipsEmptyEntries()
        => Assert.Equal(new string[] { "a", "b", "c" }, CreateSut(new Headers { new Header("list", " a, b ,,c "u8.ToArray()) }).GetHeaderStringList("list"));

    [Fact]
    public void GetInt_Digits_Parses()
        => Assert.Equal(7, CreateSut(new Headers { new Header("count", "7"u8.ToArray()) }).GetHeaderInt("count"));

    [Fact]
    public void GetInt_Invalid_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(new Headers { new Header("count", "nope"u8.ToArray()) }).GetHeaderInt("count"));

    [Fact]
    public void DecodeHeaders_RendersGuidsAsGuid_TextOtherwise_PreservingOrderAndDuplicates()
    {
        Guid id = Guid.NewGuid();
        Transport transport = CreateSut(new Headers
        {
            new Header(TransportHeaders.AggregateId, id.ToByteArray()),
            new Header("custom", "value"u8.ToArray()),
            new Header("custom", "again"u8.ToArray())
        });

        ImmutableList<KeyValuePair<string, string>> decoded = transport.DecodeHeaders();

        Assert.Equal(3, decoded.Count);
        Assert.Equal(new KeyValuePair<string, string>(TransportHeaders.AggregateId, id.ToString()), decoded[0]);
        Assert.Equal(new KeyValuePair<string, string>("custom", "value"), decoded[1]);
        Assert.Equal(new KeyValuePair<string, string>("custom", "again"), decoded[2]);
    }

    [Fact]
    public void CloneHeaders_DeepCopiesTheValues()
    {
        byte[] original = "value"u8.ToArray();
        Transport transport = CreateSut(new Headers { new Header("key", original) });

        Headers cloned = transport.CloneHeaders();
        cloned.TryGetLastBytes("key", out byte[] copy);
        copy[0] = 0;

        Assert.Equal("value"u8.ToArray(), original);
        Assert.Equal("value", transport.GetHeaderString("key"));
    }
}
