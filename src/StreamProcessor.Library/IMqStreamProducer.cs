using ByteSizeLib;
using RabbitMQ.Stream.Client;

namespace SlugEnt.StreamProcessor;

/// <summary>
/// Interface for the MQ Stream Producer class
/// </summary>
public interface IMqStreamProducer
{
    /// <summary>
    /// This tells you how many errors have been detected without a successful message.  Anytime a message is successfully confirmed this
    /// is reset to zero.  So if > 0 then multiple messages have been unsuccessful
    /// </summary>
    int ConsecutiveErrors { get; }

    /// <summary>
    /// Returns the status of the circuit breaker.  If true, message publishing is significantly diminished
    /// </summary>
    bool CircuitBreakerTripped { get; }

    /// <summary>
    /// Sets the number of consecutive message failures that occur before we stop producing any more messages
    /// </summary>
    int CircuitBreakerStopLimit { get; set; }

    /// <summary>
    /// Returns the number of messages that are waiting in the Retry Queue - Messages that failed to send previously
    /// </summary>
    int Stat_RetryQueuedMessageCount { get; }

    /// <summary>
    /// Total Number of Messages sent since startup.
    /// </summary>
    ulong SendCount { get; }

    ulong Stat_MessagesSuccessfullyConfirmed { get; }
    ulong Stat_MessagesErrored { get; }

    /// <summary>
    /// Whether this object should automatically resend failed confirmations.
    /// <para>If you turn this off no failed messages will be resent automatically.</para>">
    /// </summary>
    bool AutoRetryFailedConfirmations { get; set; }

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
    /// Performs a check to see if the circuit breaker should be reset.  Should not normally be needed
    /// But if for some reason the program gets stuck the caller should check this periodically.
    /// </summary>
    /// <returns></returns>
    bool CheckCircuitBreaker();

    /// <summary>
    /// Builds the producer.  When this call is complete the caller can begin sending messages
    /// </summary>
    /// <returns></returns>
    Task StartAsync();

    /// <summary>
    /// This creates a new message with the given message string and returns the Message object. The caller can then Add additiona KV pairs to the
    ///  ApplicationProperties and Properties Dictionaries on the Message object
    /// <para>If you do not plan to manipulate those properties then call the SendMessageAsync method directly, instead of this one.</para>
    /// </summary>
    /// <param name="messageAsString">The actual body of the message</param>
    /// <returns></returns>
    Message CreateMessage(string messageAsString);

    /// <summary>
    /// Sends the given message to the MQ Stream.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    Task<bool> SendMessageAsync(Message message);

    /// <summary>
    /// Sends the given message to the RabbitMQ stream
    /// </summary>
    /// <param name="messageAsString">The actual body of the message</param>
    /// <returns></returns>
    Task<bool> SendMessageAsync(string messageAsString);

    /// <summary>
    /// Sets no limits for the stream - It will either be controlled by RabbitMQ policies or have no limits - which is unadvisable.
    /// </summary>
    void SetNoStreamLimits();

    /// <summary>
    /// Sets the stream specifications in its raw RabbitMQ requested units of measure
    /// </summary>
    /// <param name="maxLength"></param>
    /// <param name="maxSegmentSize"></param>
    /// <param name="maxAgeInSeconds"></param>
    /// <returns></returns>
    void SetStreamLimits(ByteSize maxLength, ByteSize maxSegmentSize, TimeSpan maxAgeInSeconds);


    /// <summary>
    /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    event EventHandler<MessageConfirmationEventArgs> MessageConfirmationError;

    /// <summary>
    /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    event EventHandler<MessageConfirmationEventArgs> MessageConfirmationSuccess;

    /// <summary>
    /// Initializes the Stream
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="applicationName">This is the name of the application that owns this Stream process.
    /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
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

    Task StreamInfo();



    /// <summary>
    /// Closes the connection to MQ.
    /// </summary>
    public Task StopAsync();
}



