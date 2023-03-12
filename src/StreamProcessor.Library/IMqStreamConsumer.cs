using ByteSizeLib;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

public interface IMqStreamConsumer : IMQStreamBase
{
    /// <summary>
    /// The offset of the last message successfully received
    /// </summary>
    ulong LastOffset { get; }

    /// <summary>
    /// The offset of the last time a checkpoint was performed.  Zero indicates no checkpoint has been done yet.
    /// </summary>
    ulong CheckpointLastOffset { get; }

    /// <summary>
    /// The total number of messages consumed.
    /// </summary>
    ulong MessagesConsumed { get; }

    /// <summary>
    /// How many messages have been consumed since last Checkpoint
    /// </summary>
    int CheckpointOffsetCounter { get; }

    /// <summary>
    /// How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create your own check method.
    /// </summary>
    int CheckpointOffsetLimit { get; set; }

    /// <summary>
    /// The date time of the last Checkpoint operation.  Callers can use this if they wish to use elapsed time based checkpoint operations
    /// </summary>
    DateTime CheckpointLastDateTime { get; set; }


    /// <summary>
    /// Maximum length this stream can be.  Only applicable on newly published streams
    /// </summary>
    ByteSize MaxStreamSize { get; set; }

    /// <summary>
    /// Maximum segment size for this stream
    /// </summary>
    ByteSize MaxSegmentSize { get; set; }

    /// <summary>
    /// Max Age of records in seconds
    /// </summary>
    TimeSpan MaxAge { get; set; }


    /// <summary>
    /// Initiate the Consumption cycle
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    Task ConsumeAsync();


    /// <summary>
    /// Sets the Method to be called when a message is received.
    /// </summary>
    /// <param name="callHandler"></param>
    void SetConsumptionHandler(Func<Message, Task<bool>> callHandler);


    /// <summary>
    /// Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
    /// </summary>
    /// <returns></returns>
    Task CheckPointSetAsync();


    /// <summary>
    /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    event EventHandler<MqStreamCheckPointEventArgs> EventCheckPointSaved;
}