using ByteSizeLib;
using RabbitMQ.Stream.Client;

namespace SlugEnt.StreamProcessor;

public interface IMqStreamConsumer
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
    /// The application that owns this Stream Process.
    /// It is used when checkpointing the Stream and is tagged in the message properties when creating the message
    /// </summary>
    string ApplicationName { get; }

    /// <summary>
    /// The name of the stream we publish and consume messages from
    /// </summary>
    string MQStreamName { get; }

    /// <summary>
    /// Returns the Fullname for this MQStream.
    /// <para>Fullname is stream name combined with application name</para>">
    /// </summary>
    public string FullName { get; }


    /// <summary>
    /// Number of messages published or consumed depending on type of stream
    /// </summary>
    ulong MessageCounter { get; }

    /// <summary>
    /// Whether this stream is a publisher or consumer
    /// </summary>
    EnumMQStreamType MqStreamType { get; }

    /// <summary>
    /// Whether the stream is connected
    /// </summary>
    bool IsConnected { get; }

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

    /// <summary>
    /// Initializes the Stream
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="applicationName">This is the name of the application that owns this Stream process.
    /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
    /// <param name="mqStreamType">The type of MQ Stream</param>
    void Initialize(string mqStreamName, string applicationName, StreamSystemConfig config);

    /// <summary>
    /// Establishes a connection to the stream on the RabbitMQ server(s).
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    Task ConnectAsync();

    /// <summary>
    /// Permanently deletes the Stream off the RabbitMQ Servers.
    /// </summary>
    /// <returns></returns>
    Task DeleteStream();


    /// <summary>
    /// Stops the consumption of messages and closes the streamer.
    /// </summary>
    /// <returns></returns>
    public Task StopAsync();

    Task StreamInfo();
}
