using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests.Fakes;
using Quartz;

namespace JorgeCostaMacia.Bus.Kafka.Retry.Quartz.Tests;

public class RetryJobTests
{
    private readonly ProducerFake _producer = new();

    private Task Execute(JobDataMap data) => new RetryJob(_producer).Execute(new JobExecutionContextFake(data));

    private static string EncodeHeaders(params (string Key, byte[]? Value)[] headers)
        => JsonSerializer.Serialize(headers.Select(header => new KeyValuePair<string, byte[]?>(header.Key, header.Value)));

    private static JobDataMap Data(string topic = "orders", byte[]? body = null, string? headers = null)
        => new()
        {
            [RetryJob.TOPIC_KEY] = topic,
            [RetryJob.BODY_KEY] = Convert.ToBase64String(body ?? "body"u8.ToArray()),
            [RetryJob.HEADERS_KEY] = headers ?? "[]"
        };

    [Fact]
    public async Task Execute_ProducesTheBodyToTheTopic()
    {
        await Execute(Data(topic: "orders", body: "hello"u8.ToArray()));

        (string topic, Message<Null, byte[]> message) = Assert.Single(_producer.Produced);
        Assert.Equal("orders", topic);
        Assert.Equal("hello", Encoding.UTF8.GetString(message.Value));
    }

    [Fact]
    public async Task Execute_DecodesHeaders_PreservingNullAndDuplicates()
    {
        await Execute(Data(headers: EncodeHeaders(("k", "v"u8.ToArray()), ("n", null), ("dup", "1"u8.ToArray()), ("dup", "2"u8.ToArray()))));

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Equal("v", Encoding.UTF8.GetString(message.Headers.GetLastBytes("k")));
        Assert.Null(Assert.Single(message.Headers, header => header.Key == "n").GetValueBytes());
        Assert.Equal(2, message.Headers.Count(header => header.Key == "dup"));
    }

    [Fact]
    public async Task Execute_NoHeaders_ProducesEmptyHeaders()
    {
        await Execute(Data(headers: "[]"));

        Message<Null, byte[]> message = Assert.Single(_producer.Produced).Message;
        Assert.Empty(message.Headers);
    }

    [Fact]
    public async Task Execute_MissingTopic_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.TOPIC_KEY);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Execute_MissingBody_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.BODY_KEY);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }
}
