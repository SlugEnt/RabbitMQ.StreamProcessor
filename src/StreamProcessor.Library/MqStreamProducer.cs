using System.Collections.Concurrent;
using System.Text;
using ByteSizeLib;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace SlugEnt.StreamProcessor;

public class MqStreamProducer : MQStreamBase, IMqStreamProducer
{
    public const     int                              CIRCUIT_BREAKER_MAX_SLEEP = 180000; // 3 minutes
    public const     int                              CIRCUIT_BREAKER_MIN_SLEEP = 2000;
    public const     int                              RETRY_INITIAL_MSG_COUNT   = 3;
    private readonly object                           _retryThreadLock          = new();
    private          int                              _consecutiveErrors;
    private          Func<MessagesConfirmation, Task> _messageConfirmationHandler = null;


    protected Producer                 _producer;
    protected ConcurrentQueue<Message> _retryMessagesQueue;
    private   Thread                   _retryThread;


    /// <summary>
    ///     Builds an MQ Producer Stream
    ///     <param name="mqStreamName"></param>
    ///     <param name="applicationName">
    ///         This is the name of the application that owns this Stream process.
    ///         It must be unique as it is used when Checkpointing streams and is used as the Message source when creating
    ///         messages.
    ///     </param>
    ///     </param>
    /// </summary>
    public MqStreamProducer(ILogger<MqStreamProducer> logger, IServiceProvider serviceProvider) : base(logger, EnumMQStreamType.Producer)
    {
        _retryMessagesQueue = new ConcurrentQueue<Message>();
        ILoggerFactory        loggerFactory = serviceProvider.GetService<ILoggerFactory>();
        ILogger<StreamSystem> streamLogger  = loggerFactory.CreateLogger<StreamSystem>();
        ProducerLogger = loggerFactory.CreateLogger<Producer>();
        StreamLogger   = streamLogger;
    }


    /// <summary>
    ///     The logger for the producer.
    /// </summary>
    protected ILogger<Producer> ProducerLogger { get; set; }


    /// <summary>
    ///     Closes the producer.
    /// </summary>
    /// <returns></returns>
    public override async Task StopAsync()
    {
        await CloseStreamAsync();
        _producer.Close();
        IsConnected = false;
    }



    /// <summary>
    ///     Whether failed confirmations should automatically be resent
    ///     <para>If you turn this off no failed messages will be resent automatically.</para>
    ///     ">
    /// </summary>
    public bool AutoRetryFailedConfirmations { get; set; } = true;



    /// <summary>
    ///     Performs a check to see if the circuit breaker should be reset.  Should not normally be needed
    ///     But if for some reason the program gets stuck the caller should check this periodically.
    /// </summary>
    /// <returns></returns>
    public bool CheckCircuitBreaker()
    {
        if (!CircuitBreakerTripped)
            return CircuitBreakerTripped;

        // Are there any messages awaiting to be resent?
        if (_retryMessagesQueue.IsEmpty)
            CircuitBreakerTripped = false;

        return CircuitBreakerTripped;
    }


    /// <summary>
    ///     Sets the number of consecutive message failures that occur before we stop producing any more messages
    /// </summary>
    public int CircuitBreakerStopLimit { get; set; }


    /// <summary>
    ///     Returns the status of the circuit breaker.  If true, message publishing is significantly diminished
    /// </summary>
    public bool CircuitBreakerTripped { get; protected set; }



    /// <summary>
    ///     This tells you how many errors have been detected without a successful message.  Anytime a message is successfully
    ///     confirmed this
    ///     is reset to zero.  So if > 0 then multiple messages have been unsuccessful
    /// </summary>
    public int ConsecutiveErrors
    {
        get => _consecutiveErrors;
        protected set
        {
            _consecutiveErrors = value;
            if (_consecutiveErrors >= CircuitBreakerStopLimit)
                CircuitBreakerTripped = true;
        }
    }



    /// <summary>
    ///     This creates a new message with the given message string and returns the Message object. The caller can then Add
    ///     additiona KV pairs to the
    ///     ApplicationProperties and Properties Dictionaries on the Message object
    ///     <para>
    ///         If you do not plan to manipulate those properties then call the SendMessageAsync method directly, instead of
    ///         this one.
    ///     </para>
    /// </summary>
    /// <param name="messageAsString">The actual body of the message</param>
    /// <returns></returns>
    public Message CreateMessage(string messageAsString)
    {
        Message msg = new(Encoding.UTF8.GetBytes(messageAsString));
        msg.Properties = new Properties { CreationTime = DateTime.Now };

        msg.ApplicationProperties = new ApplicationProperties { { "Source", ApplicationName } };
        return msg;
    }


    /// <summary>
    ///     Total Number of Messages sent since startup.
    /// </summary>
    public ulong SendCount => MessageCounter;



    /// <summary>
    ///     Sends the given message to the MQ Stream.  If false is returned you should ensure the producer is connected and
    ///     CircuitBreaker not tripped.
    /// </summary>
    /// <param name="message"></param>
    /// <returns>False if it failed to send message</returns>
    public async Task<bool> SendMessageAsync(Message message) => await SendMessageAsync(message, false);


    /// <summary>
    ///     Sends the given string message to the RabbitMQ stream.  String msg will be converted into a full message object
    ///     <para>If false is returned you should check circuit breaker.  Also ensure producer is connected to MQ</para>
    /// </summary>
    /// <param name="messageAsString">The actual body of the message</param>
    /// <returns>False if it failed to send message</returns>
    public async Task<bool> SendMessageAsync(string messageAsString)
    {
        Message message = new(Encoding.UTF8.GetBytes(messageAsString));
        return await SendMessageAsync(message);
    }


    /// <summary>
    ///     Sets no limits for the stream - It will either be controlled by RabbitMQ policies or have no limits - which is
    ///     unadvisable.
    /// </summary>
    public void SetNoStreamLimits() { _streamSpec = new StreamSpec(_mqStreamName); }



    // TODO - No idea why SetStreamLimitsAsync needs to be async.  Remove async


    /// <summary>
    ///     Sets the Stream Limits in more typical units of measure
    /// </summary>
    /// <param name="maxBytesInMb"></param>
    /// <param name="maxSegmentSizeInMb"></param>
    /// <param name="maxAgeInHours"></param>
    /// <returns></returns>
    public void SetStreamLimits(ByteSize maxStreamSize, ByteSize maxSegmentSizeInMb, TimeSpan maxAge)
    {
        // Convert Values to bytes
        MaxStreamSize  = maxStreamSize;
        MaxSegmentSize = maxSegmentSizeInMb;
        MaxAge         = maxAge;

        GenerateStreamSpec();
    }



    /// <summary>
    ///     Builds the producer.  When this call is complete the caller can begin sending messages
    /// </summary>
    /// <returns></returns>
    public async Task StartAsync()
    {
        _producer = await Producer.Create(
                                          new ProducerConfig(_streamSystem, _mqStreamName)
                                          {
                                              // TODO revist this.  It may be necessary if more than 1 program is producing....
                                              // Is not necessary if sending from 1 thread.
                                              //Reference = Guid.NewGuid().ToString(),

                                              ConfirmationHandler = OnConfirmation
                                          }, ProducerLogger);
    }


    /// <summary>
    ///     Number of messages that have been confirmed in error
    /// </summary>
    public ulong Stat_MessagesErrored { get; private set; }


    /// <summary>
    ///     The number of messages that have been successfully confirmed.
    /// </summary>
    public ulong Stat_MessagesSuccessfullyConfirmed { get; private set; }


    /// <summary>
    ///     Returns the number of messages that are waiting in the Retry Queue - Messages that failed to send previously
    /// </summary>
    public int Stat_RetryQueuedMessageCount => _retryMessagesQueue.Count;



    /// <summary>
    ///     Processes the Confirmation.
    ///     <para>
    ///         If caller has indicated they want successful confirmations then the event MessageConfirmationSuccess is
    ///         raised
    ///     </para>
    ///     <para>On errors the Event MessageConfirmationError is raised</para>
    ///     <remarks>
    ///         This is broken out from OnConfirmation, because the MessagesConfirmation variable is unable to be moq'd
    ///         for testing purposes
    ///     </remarks>
    /// </summary>
    /// <param name="status"></param>
    /// <param name="messages"></param>
    protected void ConfirmationProcessor(ConfirmationStatus status, List<Message> messages)
    {
        bool newErrors = false;

        foreach (Message message in messages)
        {
            if (status == ConfirmationStatus.Confirmed)
            {
                ConsecutiveErrors     = 0;
                CircuitBreakerTripped = false;
                Stat_MessagesSuccessfullyConfirmed++;

                if (MessageConfirmationSuccess != null)
                {
                    MessageConfirmationEventArgs eventArgs = new();
                    eventArgs.Status  = status;
                    eventArgs.Message = message;
                    OnConfirmationSuccess(eventArgs);
                }
            }
            else
            {
                ConsecutiveErrors++;
                Stat_MessagesErrored++;
                newErrors = true;

                if (message.ApplicationProperties != null)
                {
                    if (message.ApplicationProperties.ContainsKey(AP_RETRIES))
                        message.ApplicationProperties[AP_RETRIES] =
                            (int)message.ApplicationProperties[AP_RETRIES] + 1;
                    else
                        message.ApplicationProperties.Add(AP_RETRIES, 1);
                }

                _retryMessagesQueue.Enqueue(message);

                MessageConfirmationEventArgs eventArgs = new();
                eventArgs.Status  = status;
                eventArgs.Message = message;
                OnConfirmationError(eventArgs);
            }
        }

        if (newErrors)

            // Start the Retry Thread if not already running.
            if (AutoRetryFailedConfirmations)
                TurnAutoRetryThreadOn();
    }



    /// <summary>
    ///     Creates the Stream Specifications - Max age of messages, Max size of the stream, and the max size of a segment file
    /// </summary>
    /// <exception cref="ArgumentException"></exception>
    private void GenerateStreamSpec()
    {
        if (MaxStreamSize == ByteSize.FromBytes(0))
            throw new ArgumentException(
                                        "maxLength is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
        if (MaxSegmentSize == ByteSize.FromBytes(0))
            throw new ArgumentException(
                                        "maxSegmentSize is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
        if (MaxAge == TimeSpan.Zero)
            throw new ArgumentException(
                                        "maxAge is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");


        _streamSpec = new StreamSpec(_mqStreamName)
        {
            MaxAge = MaxAge, MaxLengthBytes = (ulong)MaxStreamSize.Bytes, MaxSegmentSizeBytes = (int)MaxSegmentSize.Bytes
        };
    }



    /// <summary>
    ///     Handles message confirmations.  If the user has supplied their own then this method will
    ///     handle the errors and then pass onto the user supplied method.
    /// </summary>
    /// <param name="confirmation"></param>
    /// <returns></returns>
    protected Task OnConfirmation(MessagesConfirmation confirmation)
    {
        ConfirmationProcessor(confirmation.Status, confirmation.Messages);

        return Task.CompletedTask;
    }



    /// <summary>
    ///     Attempts to send the specified number of messages from the Retry Queue.
    ///     <para> Note, this bypasses the Circuit Breaker check in SendMessageAsync.  It will ALWAYS attempt to send to MQ</para>
    /// </summary>
    /// <param name="maxMessages"></param>
    /// <returns></returns>
    private bool ResendMessages(int maxMessages)
    {
        for (int i = 0; i < maxMessages; i++)
        {
            // If no more messages available then exit
            if (!_retryMessagesQueue.TryDequeue(out Message msg))
                return false;

            // Send the message
            SendMessageAsync(msg, true);
        }

        return true;
    }


    /// <summary>
    ///     Sends the given message to the MQ Stream.  If this returns false, then the CircuitBreaker has been set.
    /// </summary>
    /// <param name="message">Message to send</param>
    /// <param name="bypassCircuitBreaker">If true, the Circuit breaker will be bypassed.  Used during Resends</param>
    /// <returns>False if it failed to send message</returns>
    protected async Task<bool> SendMessageAsync(Message message, bool bypassCircuitBreaker = false)
    {
        if (!CircuitBreakerTripped || bypassCircuitBreaker)
        {
            await SendMessageToMQAsync(message);
            MessageCounter++;
            return true;
        }

        bool  keepChecking = true;
        short loopCounter  = 0;
        while (keepChecking && loopCounter < 4)
        {
            loopCounter++;
            if (CircuitBreakerTripped)
            {
                Thread.Sleep(200);
            }
            else
            {
                await SendMessageToMQAsync(message);
                MessageCounter++;
                return true;
            }
        }

        // Failed to send the message.
        return false;
    }



    /// <summary>
    ///     Sends the message to RabbitMQ.
    ///     <remarks>This is broken out into its own method so it can be overriden for unit testing.</remarks>
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    protected virtual async Task SendMessageToMQAsync(Message message) { await _producer.Send(message); }


    /// <summary>
    ///     This is temporary so I can continue to use my sample queue.
    /// </summary>
    /// <returns></returns>
    public async Task SetStreamLimitsSmallAsync()
    {
        _streamSpec = new StreamSpec(_mqStreamName) { MaxAge = TimeSpan.FromHours(2), MaxLengthBytes = 20000, MaxSegmentSizeBytes = 10000 };
    }



    /// <summary>
    ///     Processes Queued Retry messages
    /// </summary>
    private void ThreadedRetryMessages()
    {
        // We retry a max of 3 messages.
        int attempts        = 0;
        int maxMessages     = RETRY_INITIAL_MSG_COUNT;
        int sleepTime       = CIRCUIT_BREAKER_MIN_SLEEP;
        int stopThreadAfter = 0;

        // TODO - Need to do something better than true
        while (true)
        {
            bool moreMessages = ResendMessages(maxMessages);

            if (moreMessages)
            {
                // If Circuit Breaker still tripped then sleep an increasing amount of time.
                if (CircuitBreakerTripped)
                {
                    sleepTime += sleepTime;
                    if (sleepTime > CIRCUIT_BREAKER_MAX_SLEEP)
                        sleepTime = CIRCUIT_BREAKER_MAX_SLEEP;
                    Thread.Sleep(sleepTime);
                }
                else
                {
                    // Supposedly the problem is fixed, so gradually send more messages and sleep small amount of time 
                    maxMessages = 30;
                    sleepTime   = CIRCUIT_BREAKER_MIN_SLEEP;
                    Thread.Sleep(1000);
                }
            }
            else
            {
                lock (_retryThreadLock)
                {
                    _retryThread = null;
                    return;
                }
            }

            //Thread.Sleep(CIRCUIT_BREAKER_NORMAL_SLEEP);
        }
    }



    /// <summary>
    ///     Turns the Auto-Retry Thread on.
    /// </summary>
    protected void TurnAutoRetryThreadOn()
    {
        lock (_retryThreadLock)
        {
            if (_retryThread == null)
            {
                _retryThread              = new Thread(ThreadedRetryMessages);
                _retryThread.IsBackground = true;
                _retryThread.Start();
            }
        }
    }


    //##############################################    ########################################################
    //######################################################################################################


#region "Events"

    // Message Confirmation Error Handling
    /// <summary>
    ///     IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    public event EventHandler<MessageConfirmationEventArgs> MessageConfirmationError;


    protected virtual void OnConfirmationError(MessageConfirmationEventArgs e)
    {
        EventHandler<MessageConfirmationEventArgs> handler = MessageConfirmationError;
        if (handler != null)
            handler(this, e);
    }



    // Message Confirmation Success Handling
    /// <summary>
    ///     IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    public event EventHandler<MessageConfirmationEventArgs> MessageConfirmationSuccess;


    protected virtual void OnConfirmationSuccess(MessageConfirmationEventArgs e)
    {
        EventHandler<MessageConfirmationEventArgs> handler = MessageConfirmationSuccess;
        if (handler != null)
            handler(this, e);
    }

#endregion
}


public class MessageConfirmationEventArgs : EventArgs
{
    public Message Message { get; set; }
    public ConfirmationStatus Status { get; set; }
}