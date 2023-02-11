using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Text;
using RabbitMQ.Stream.Client.AMQP;

namespace SlugEnt.StreamProcessor
{
    public class MqStreamProducer : MQStreamBase
    {
        protected Producer _producer;
        private Func<MessagesConfirmation, Task> _messageConfirmationHandler = null;
        private int _consecutiveErrors;
        private bool _circuitBreakerTripped = false;


        /// <summary>
        /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
        /// </summary>
        public event Action<MessagesConfirmation> ConfirmationErrorEvent;


        /// <summary>
        /// Builds an MQ Producer Stream
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// </param>
        /// </summary>
        public MqStreamProducer(string mqStreamName, string applicationName) : base(mqStreamName,applicationName,EnumMQStreamType.Producer)
        {
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
        }


        /// <summary>
        /// Sets the number of consecutive message failures that occur before we stop producing any more messages
        /// </summary>
        public int CircuitBreakerStopLimit { get; set; }



        /// <summary>
        /// Total Number of Messages sent since startup.
        /// </summary>
        public ulong SendCount
        {
            get { return MessageCounter;}
        }


        /// <summary>
        /// This method sets the method that is called when a message has been successfully confirmed.
        /// <para>Only set this if you need to do something with the confirmed message.</para>
        /// </summary>
        /// <param name="confirmationHandler"></param>
        public void SetConfirmationHandler(Func<MessagesConfirmation, Task> confirmationHandler)
        {
            _messageConfirmationHandler = confirmationHandler;
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
        /// Sends the given message to the MQ Stream.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task SendMessage(Message message)
        {
            try
            {
                await _producer.Send(message);
                MessageCounter++;
            }
            catch (Exception ex)
            {
                // TODO Log the error
            }
        }


        /// <summary>
        /// Sends the given message to the RabbitMQ stream
        /// </summary>
        /// <param name="messageAsString">The actual body of the message</param>
        /// <returns></returns>
        public async Task SendMessage(string messageAsString)
        {
            var message = new Message(Encoding.UTF8.GetBytes(messageAsString));
            await SendMessage(message);
        }



        /// <summary>
        /// Handles message confirmations.  If the user has supplied their own then this method will
        /// handle the errors and then pass onto the user supplied method.
        /// </summary>
        /// <param name="confirmation"></param>
        /// <returns></returns>
        protected Task OnConfirmation (MessagesConfirmation confirmation)
        {
            if (confirmation.Status == ConfirmationStatus.Confirmed)
            {
                ConsecutiveErrors = 0;
                if (_messageConfirmationHandler != null)
                {
                    _messageConfirmationHandler(confirmation);
                }
            }
            else
            {
                ConsecutiveErrors++;
                ConfirmationErrorEvent(confirmation);
/*                switch (confirmation.Status)
                {
                    
                    // There is an error during the sending of the message
                    case ConfirmationStatus.WaitForConfirmation:
                    case ConfirmationStatus.ClientTimeoutError
                        : // The client didn't receive the confirmation in time. 
                    // but it doesn't mean that the message was not sent
                    // maybe the broker needs more time to confirm the message
                    // see TimeoutMessageAfter in the ProducerConfig
                    case ConfirmationStatus.StreamNotAvailable:
                    case ConfirmationStatus.InternalError:
                    case ConfirmationStatus.AccessRefused:
                    case ConfirmationStatus.PreconditionFailed:
                    case ConfirmationStatus.PublisherDoesNotExist:
                    case ConfirmationStatus.UndefinedError:
                    default:
                        Console.WriteLine(
                            $"Message  {confirmation.PublishingId} not confirmed. Error {confirmation.Status}");
                        break;
                }
*/
            }

            return Task.CompletedTask;
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


    }
}
