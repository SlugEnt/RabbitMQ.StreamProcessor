using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client.AMQP;

namespace SlugEnt.StreamProcessor
{
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
        ulong MaxLength { get; set; }

        /// <summary>
        /// Maximum segment size for this stream
        /// </summary>
        int MaxSegmentSize { get; set; }

        /// <summary>
        /// Max Age of records in seconds
        /// </summary>
        ulong MaxAge { get; set; }

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
        /// <para>If you do not plan to manipulate those properties then call the SendMessage method directly, instead of this one.</para>
        /// </summary>
        /// <param name="messageAsString">The actual body of the message</param>
        /// <returns></returns>
        Message CreateMessage(string messageAsString);

        /// <summary>
        /// Sends the given message to the MQ Stream.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        Task<bool> SendMessage(Message message);

        /// <summary>
        /// Sends the given message to the RabbitMQ stream
        /// </summary>
        /// <param name="messageAsString">The actual body of the message</param>
        /// <returns></returns>
        Task<bool> SendMessage(string messageAsString);

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
        Task SetStreamLimitsRaw(ulong maxLength, int maxSegmentSize, ulong maxAgeInSeconds);

        /// <summary>
        /// Sets the Stream Limits in more typical units of measure
        /// </summary>
        /// <param name="maxBytesInMb"></param>
        /// <param name="maxSegmentSizeInMb"></param>
        /// <param name="maxAgeInHours"></param>
        /// <returns></returns>
        Task SetStreamLimits(int maxBytesInMb = 1, int maxSegmentSizeInMb = 1, ulong maxAgeInHours = 24);

        /// <summary>
        /// This is temporary so I can continue to use my sample queue.  
        /// </summary>
        /// <returns></returns>
        Task SetStreamLimitsSmall();

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
    }

    public class MqStreamProducer : MQStreamBase, IMqStreamProducer
    {
        public const int CIRCUIT_BREAKER_MIN_SLEEP = 2000;
        public const int CIRCUIT_BREAKER_MAX_SLEEP = 180000;  // 3 minutes
        public const int RETRY_INITIAL_MSG_COUNT = 3;
        

        protected Producer _producer;
        private Func<MessagesConfirmation, Task> _messageConfirmationHandler = null;
        private int _consecutiveErrors;
        private bool _circuitBreakerTripped = false;
        protected ConcurrentQueue<Message> _retryMessagesQueue;
        private Thread _retryThread = null;
        private readonly object _retryThreadLock = new object();


        /// <summary>
        /// Builds an MQ Producer Stream
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// </param>
        /// </summary>
        public MqStreamProducer(ILogger<MqStreamProducer> logger) : base(logger, EnumMQStreamType.Producer)
        {
            _retryMessagesQueue = new ConcurrentQueue<Message>();
        }


        /// <summary>
        /// This tells you how many errors have been detected without a successful message.  Anytime a message is successfully confirmed this
        /// is reset to zero.  So if > 0 then multiple messages have been unsuccessful
        /// </summary>
        public int ConsecutiveErrors
        {
            get { return _consecutiveErrors;}
            protected set
            {
                _consecutiveErrors = value;
                if (_consecutiveErrors >= CircuitBreakerStopLimit)
                    _circuitBreakerTripped = true;
            }
        }


        /// <summary>
        /// Returns the status of the circuit breaker.  If true, message publishing is significantly diminished
        /// </summary>
        public bool CircuitBreakerTripped
        {
            get { return _circuitBreakerTripped; }
            protected set
            {
                _circuitBreakerTripped = value;
            }
        }


        /// <summary>
        /// Sets the number of consecutive message failures that occur before we stop producing any more messages
        /// </summary>
        public int CircuitBreakerStopLimit { get; set; }


        /// <summary>
        /// Returns the number of messages that are waiting in the Retry Queue - Messages that failed to send previously
        /// </summary>
        public int Stat_RetryQueuedMessageCount
        {
            get { return _retryMessagesQueue.Count; }
        }

        /// <summary>
        /// Total Number of Messages sent since startup.
        /// </summary>
        public ulong SendCount
        {
            get { return MessageCounter;}
        }


        public ulong Stat_MessagesSuccessfullyConfirmed
        {
            get;
            private set;
        }

        public ulong Stat_MessagesErrored { get; private set; }

        /// <summary>
        /// Whether this object should automatically resend failed confirmations.
        /// <para>If you turn this off no failed messages will be resent automatically.</para>">
        /// </summary>
        public bool AutoRetryFailedConfirmations { get; set; } = true;



        /// <summary>
        /// Performs a check to see if the circuit breaker should be reset.  Should not normally be needed
        /// But if for some reason the program gets stuck the caller should check this periodically.
        /// </summary>
        /// <returns></returns>
        public bool CheckCircuitBreaker()
        {
            if (! CircuitBreakerTripped) return CircuitBreakerTripped;

            // Are there any messages awaiting to be resent?
            if (_retryMessagesQueue.IsEmpty) CircuitBreakerTripped = false;

            return CircuitBreakerTripped;
        }



        /// <summary>
        /// Builds the producer.  When this call is complete the caller can begin sending messages
        /// </summary>
        /// <returns></returns>
        public async Task StartAsync()
        {
            _producer = await Producer.Create(
                new ProducerConfig(_streamSystem, _mqStreamName)
                {
                    // Is not necessary if sending from 1 thread.
                    //Reference = Guid.NewGuid().ToString(),


                    ConfirmationHandler = OnConfirmation
                });

        }



        /// <summary>
        /// This creates a new message with the given message string and returns the Message object. The caller can then Add additiona KV pairs to the
        ///  ApplicationProperties and Properties Dictionaries on the Message object
        /// <para>If you do not plan to manipulate those properties then call the SendMessage method directly, instead of this one.</para>
        /// </summary>
        /// <param name="messageAsString">The actual body of the message</param>
        /// <returns></returns>
        public Message CreateMessage(string messageAsString)
        {
            Message msg = new Message(Encoding.UTF8.GetBytes(messageAsString));
            msg.Properties = new Properties()
            {
                CreationTime = DateTime.Now
            };

            msg.ApplicationProperties = new ApplicationProperties
            {
                { "Source", ApplicationName }
            };
            return msg;
        }



        /// <summary>
        /// Sends the message to RabbitMQ.
        /// <remarks>This is broken out into its own method so it can be overriden for unit testing.</remarks>
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        protected virtual async Task SendMessageToMQ(Message message)
        {
            await _producer.Send(message);
        }


        /// <summary>
        /// Sends the given message to the MQ Stream.  If this returns false, then the CircuitBreaker has been set.
        /// </summary>
        /// <param name="message">Message to send</param>
        /// <param name="bypassCircuitBreaker">If true, the Circuit breaker will be bypassed.  Used during Resends</param>
        /// <returns></returns>
        protected async Task<bool> SendMessage(Message message, bool bypassCircuitBreaker = false)
        {
                if (!CircuitBreakerTripped || bypassCircuitBreaker)
                {
                    await SendMessageToMQ(message);
                    MessageCounter++;
                    return true;
                }
                else
                {
                    bool keepChecking = true;
                    short loopCounter = 0;
                    while (keepChecking && loopCounter < 4)
                    {
                        loopCounter++;
                        if (CircuitBreakerTripped) Thread.Sleep(200);
                        else
                        {
                            await SendMessageToMQ(message);
                            MessageCounter++;
                            return true;
                        }
                    }
                    // Failed to send the message.
                    return false;
                }
        }



        /// <summary>
        /// Sends the given message to the MQ Stream.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<bool> SendMessage(Message message)
        {
            return await SendMessage(message, false);
        }


        /// <summary>
        /// Sends the given message to the RabbitMQ stream
        /// </summary>
        /// <param name="messageAsString">The actual body of the message</param>
        /// <returns></returns>
        public async Task<bool> SendMessage(string messageAsString)
        {
            Message message = new Message(Encoding.UTF8.GetBytes(messageAsString));
            return await SendMessage(message);
        }



        /// <summary>
        /// Handles message confirmations.  If the user has supplied their own then this method will
        /// handle the errors and then pass onto the user supplied method.
        /// </summary>
        /// <param name="confirmation"></param>
        /// <returns></returns>
        protected Task OnConfirmation (MessagesConfirmation confirmation)
        {
            ConfirmationProcessor(confirmation.Status,confirmation.Messages);

            return Task.CompletedTask;
        }



        /// <summary>
        /// Processes the Confirmation.
        /// <para>If caller has indicated they want successful confirmations then the event MessageConfirmationSuccess is raised</para>
        /// <para>On errors the Event MessageConfirmationError is raised</para>
        /// <remarks>This is broken out from OnConfirmation, because the MessagesConfirmation variable is unable to be moq'd for testing purposes</remarks>
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
                    ConsecutiveErrors = 0;
                    CircuitBreakerTripped = false;
                    Stat_MessagesSuccessfullyConfirmed++;

                    if (MessageConfirmationSuccess != null)
                    {
                        MessageConfirmationEventArgs eventArgs = new MessageConfirmationEventArgs();
                        eventArgs.Status = status;
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
                        if (message.ApplicationProperties.ContainsKey(MQStreamBase.AP_RETRIES))
                            message.ApplicationProperties[MQStreamBase.AP_RETRIES] =
                                (int)message.ApplicationProperties[MQStreamBase.AP_RETRIES] + 1;
                        else
                            message.ApplicationProperties.Add(MQStreamBase.AP_RETRIES, 1);
                    }

                    _retryMessagesQueue.Enqueue(message);

                    MessageConfirmationEventArgs eventArgs = new MessageConfirmationEventArgs();
                    eventArgs.Status = status;
                    eventArgs.Message = message;
                    OnConfirmationError(eventArgs);

                }
            }

            if (newErrors) {
                // Start the Retry Thread if not already running.
                if (AutoRetryFailedConfirmations)
                {
                    TurnAutoRetryThreadOn();
                }
            }
        }



        /// <summary>
        /// Turns the Auto-Retry Thread on.
        /// </summary>
        protected void TurnAutoRetryThreadOn()
        {
            lock (_retryThreadLock)
            {
                if (_retryThread == null)
                {
                    _retryThread = new Thread(ThreadedRetryMessages);
                    _retryThread.IsBackground = true;
                    _retryThread.Start();
                }
            }

        }

        /// <summary>
        /// Sets no limits for the stream - It will either be controlled by RabbitMQ policies or have no limits - which is unadvisable.
        /// </summary>
        public void SetNoStreamLimits()
        {
            _streamSpec = new StreamSpec(_mqStreamName);
        }


        /// <summary>
        /// Sets the stream specifications in its raw RabbitMQ requested units of measure
        /// </summary>
        /// <param name="maxLength"></param>
        /// <param name="maxSegmentSize"></param>
        /// <param name="maxAgeInSeconds"></param>
        /// <returns></returns>
        public async Task SetStreamLimitsRaw(ulong maxLength, int maxSegmentSize, ulong maxAgeInSeconds)
        {
            MaxAge = maxAgeInSeconds;
            MaxLength = maxLength;
            MaxSegmentSize = maxSegmentSize;
            GenerateStreamSpec();
        }



        /// <summary>
        /// Sets the Stream Limits in more typical units of measure
        /// </summary>
        /// <param name="maxBytesInMb"></param>
        /// <param name="maxSegmentSizeInMb"></param>
        /// <param name="maxAgeInHours"></param>
        /// <returns></returns>
        public async Task SetStreamLimits(int maxBytesInMb = 1, int maxSegmentSizeInMb = 1, ulong maxAgeInHours = 24)
        {
            // Convert Values to bytes
            MaxLength = (ulong)maxBytesInMb * 1024 * 1024;
            MaxSegmentSize = maxSegmentSizeInMb * 1024 * 1024;
            MaxAge = maxAgeInHours * 24 * 60 * 60;

            GenerateStreamSpec();
        }



        /// <summary>
        /// Creates the Stream Specifications - Max age of messages, Max size of the stream, and the max size of a segment file
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        private void GenerateStreamSpec()
        {
            if (MaxLength == 0)
                throw new ArgumentException(
                    "maxLength is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxSegmentSize == 0)
                throw new ArgumentException(
                    "maxSegmentSize is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxAge == 0)
                throw new ArgumentException(
                    "maxAge is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");


            TimeSpan maxAge = TimeSpan.FromSeconds(MaxAge);

            _streamSpec = new StreamSpec(_mqStreamName)
            {
                MaxAge = maxAge,
                MaxLengthBytes = MaxLength,
                MaxSegmentSizeBytes = MaxSegmentSize
            };
        }


        /// <summary>
        /// This is temporary so I can continue to use my sample queue.  
        /// </summary>
        /// <returns></returns>
        public async Task SetStreamLimitsSmall()
        {
            _streamSpec = new StreamSpec(_mqStreamName)
            {
                MaxAge = TimeSpan.FromHours(2),
                MaxLengthBytes = 20000,
                MaxSegmentSizeBytes = 10000
            };
        }



        /// <summary>
        /// Processes Queued Retry messages
        /// </summary>
        private void ThreadedRetryMessages()
        {
            // We retry a max of 3 messages.
            int attempts = 0;
            int maxMessages = RETRY_INITIAL_MSG_COUNT;
            int sleepTime = CIRCUIT_BREAKER_MIN_SLEEP;
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
                        if (sleepTime > CIRCUIT_BREAKER_MAX_SLEEP) sleepTime = CIRCUIT_BREAKER_MAX_SLEEP;
                        Thread.Sleep(sleepTime);
                    }
                    else
                    {
                        // Supposedly the problem is fixed, so gradually send more messages and sleep small amount of time 
                        maxMessages = 30;
                        sleepTime = CIRCUIT_BREAKER_MIN_SLEEP;
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
        /// Attempts to send the specified number of messages from the Retry Queue.
        /// <para> Note, this bypasses the Circuit Breaker check in SendMessage.  It will ALWAYS attempt to send to MQ</para>
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
                else
                    // Send the message
                    SendMessage(msg,true);
            }

            return true;
        }


        //##############################################    ########################################################
        //######################################################################################################
        #region "Events"
        // Message Confirmation Error Handling
        /// <summary>
        /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
        /// </summary>
        public event EventHandler<MessageConfirmationEventArgs> MessageConfirmationError;

        protected virtual void OnConfirmationError(MessageConfirmationEventArgs e)
        {
            EventHandler<MessageConfirmationEventArgs> handler = MessageConfirmationError;
            if (handler != null) handler(this, e);
        }



        // Message Confirmation Success Handling
        /// <summary>
        /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
        /// </summary>
        public event EventHandler<MessageConfirmationEventArgs> MessageConfirmationSuccess;

        protected virtual void OnConfirmationSuccess(MessageConfirmationEventArgs e)
        {
            EventHandler<MessageConfirmationEventArgs> handler = MessageConfirmationSuccess;
            if (handler != null) handler(this, e);
        }
        #endregion

    }

    public class MessageConfirmationEventArgs : EventArgs
    {
        public ConfirmationStatus Status { get; set; }
        public Message Message { get; set; }
    }
}
