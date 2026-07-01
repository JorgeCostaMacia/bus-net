namespace JorgeCostaMacia.Bus.Domain.Contexts;

/// <summary>
/// Envelope facet exposing the transport acknowledgement of the inbound message. The concrete
/// context maps the transport's native ack (a RabbitMQ delivery tag, a Kafka
/// <c>TopicPartitionOffset</c>, …) to <typeparamref name="TAck"/> — typically your own neutral type
/// — so consumers stay decoupled from the messaging system. Parameterised only by
/// <typeparamref name="TAck"/>, so the ack can be read without knowing the message type.
/// </summary>
/// <typeparam name="TAck">The (neutral) acknowledgement type the transport maps its native ack to.</typeparam>
public interface IAckMessageContext<TAck> : IMessageContext
{
    /// <summary>Returns the acknowledgement information for the inbound message.</summary>
    /// <returns>The acknowledgement, as mapped by the transport to <typeparamref name="TAck"/>.</returns>
    TAck GetAck();
}

/// <summary>The ack envelope facet bound to a specific inbound message type.</summary>
/// <typeparam name="TAck">The (neutral) acknowledgement type the transport maps its native ack to.</typeparam>
/// <typeparam name="TMessage">The type of the delivered message.</typeparam>
public interface IAckMessageContext<TAck, TMessage> : IAckMessageContext<TAck>, IMessageContext<TMessage>
    where TMessage : IMessage
{ }
