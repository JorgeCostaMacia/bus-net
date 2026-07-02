using JorgeCostaMacia.Bus.Command.Domain;
using JorgeCostaMacia.Bus.Kafka.Domain;

namespace JorgeCostaMacia.Bus.Kafka.Infrastructure;

/// <summary>
/// The topic mapping for a command type (type → topic routing). The topic itself is infrastructure —
/// auto-created by the broker and managed broker-side.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public sealed record CommandConfiguration<TCommand> : IMessageConfiguration
    where TCommand : ICommand
{
    /// <summary>The CLR type of the command.</summary>
    public Type MessageType { get; init; }

    /// <summary>The Kafka topic the command is sent to.</summary>
    public string Topic { get; init; }

    /// <summary>Maps the command type to its topic.</summary>
    /// <param name="topic">The Kafka topic.</param>
    public CommandConfiguration(string topic)
    {
        MessageType = typeof(TCommand);
        Topic = topic;
    }
}
