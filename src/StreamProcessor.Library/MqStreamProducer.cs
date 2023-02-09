using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Text;
using RabbitMQ.Stream.Client.AMQP;

namespace SlugEnt.StreamProcessor
{
    public class MqStreamProducer : MQStreamBase
    {
        Producer _producer;


        /// <summary>
        /// Builds an MQ Producer Stream
        /// </summary>
        /// <param name="name"></param>
        public MqStreamProducer(string mqStreamName) : base(mqStreamName,EnumMQStreamType.Producer)
        {
        }


        /// <summary>
        /// Total Number of Messages sent since startup.
        /// </summary>
        public ulong SendCount
        {
            get { return MessageCounter;}
        }





        /// <summary>
        /// Builds the producer.  When this call is complete the caller can begin sending messages
        /// </summary>
        /// <returns></returns>
        public async Task StartAsync()
        {
            _producer = await Producer.Create(
                new ProducerConfig(_streamSystem, _MQStreamName)
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
            return new Message(Encoding.UTF8.GetBytes(messageAsString));
        }



        /// <summary>
        /// Sends the given message to the MQ Stream
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
        /// When a message has been confirmed this method is called.
        /// </summary>
        /// <param name="confirmation"></param>
        /// <returns></returns>
        public Task OnConfirmation (MessagesConfirmation confirmation)
        {
            switch (confirmation.Status)
            {
                // ConfirmationStatus.Confirmed: The message was successfully sent
                case ConfirmationStatus.Confirmed:
                    //Console.WriteLine($"Message {confirmation.PublishingId} Publisher Confirmed!");
                    break;
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

            return Task.CompletedTask;
        }


        /// <summary>
        /// Sets no limits for the stream - It will either be controlled by RabbitMQ policies or have no limits - which is unadvisable.
        /// </summary>
        public void SetNoStreamLimits()
        {
            _streamSpec = new StreamSpec(_MQStreamName);
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

            _streamSpec = new StreamSpec(_MQStreamName)
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
            _streamSpec = new StreamSpec(_MQStreamName)
            {
                MaxAge = TimeSpan.FromHours(2),
                MaxLengthBytes = 20000,
                MaxSegmentSizeBytes = 10000
            };
        }


    }
}
