using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace Test_StreamProcessorLibrary;


/// <summary>
/// This class is derived from the MqStreamConsumer.  It is used to enable the unit testing of some of the RabbitMQ functionality.
/// For instance, SendingMessages actually just sends to a queue.
/// Also some classes are unable to be instantiated outside the RabbitMQ Streams client.  This bypasses that requirement.
/// </summary>
public class MqTesterConsumer : MqStreamConsumer
{
    // Simulating MQ
    private Queue<Message> _messagesToConsume = new Queue<Message>();
    private ulong _msgOffsetID = 0;
    private ulong _rabbitMQ_StoredOffset = 0;


    /// <summary>
    /// Simulates a MQ Stream instance.  Can publish, publish confirm and consume messages.
    /// </summary>
    /// <param name="streamName"></param>
    /// <param name="appName"></param>
    public MqTesterConsumer(ILogger<MqTesterConsumer> logger) : base(logger)
    {
        // Automatically assume we are connected.
        IsConnected = true;
        SetConsumptionHandler(ConsumptionHandler);
    }

    public ulong SetStartingOffset
    {
        get { return _msgOffsetID;}
        set { _msgOffsetID = value; }
    }


    public Queue<Message> ConsumerMessageQueue
    {
        get { return _messagesToConsume; }
    }

    public Queue<Message> ProducerMessageQueue
    {
        get; set;
    }


    /// <summary>
    /// Override the storage of the offset.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    protected override async Task RabbitMQ_StoreOffsetAsync(ulong offset)
    {
        _rabbitMQ_StoredOffset = offset;
    }


    /// <summary>
    /// Override the Query Offset Method
    /// </summary>
    /// <returns></returns>
    protected override async Task<IOffsetType> RabbitMQQueryOffsetAsync()
    {
        IOffsetType offsetType = new OffsetTypeOffset(_msgOffsetID);
        return offsetType;
    }


    /// <summary>
    /// 
    /// </summary>
    /// <param name="qtyToDequeu"></param>
    public async Task TST_ConsumeMessages(int qtyToDequeu = 1)
    {
        while (ProducerMessageQueue.Count > 0)
        {
            _messagesToConsume.Enqueue(ProducerMessageQueue.Dequeue());
            
        }

        foreach (Message message in _messagesToConsume)
        {
            MessageContext msgContext = new(++_msgOffsetID,TimeSpan.FromMilliseconds(-1));
            await ProcessMessageAsync(msgContext, message);
        }
    }



    /// <summary>
    /// Dummy Consumption handler.  We will just store it into a queue
    /// </summary>
    /// <param name=""></param>
    /// <returns></returns>
    private async Task<bool> ConsumptionHandler(Message message)
    {
        return true;
    }
}
