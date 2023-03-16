using ByteSizeLib;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

/// <summary>
/// Interface for the MQ Stream Producer class
/// </summary>
public interface IMqStreamProducer : IMQStreamBase
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
    /// <param name="contentType">The mime type of the body.</param>
    /// <returns></returns>
    Message CreateMessage(string messageAsString, string contentType);


    Message CreateMessage<T>(T valueToEncode);


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
}