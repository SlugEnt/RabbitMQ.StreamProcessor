
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Text;

namespace SlugEnt.StreamProcessor
{
    public class StreamConsumer : StreamBase
    {
        //private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;
        private Func<Message, Task<bool>> _callHandler;
        private readonly string _consumerApplicationName;
        private IConsumer _rawConsumer;

        /// <summary>
        /// Creates a Stream Consumer object
        /// </summary>
        /// <param name="streamName">The name of the RabbitMQ Queue (stream) name to receive messages from</param>
        /// <param name="consumerApplicationName">Unique name for the application processing this stream.  This is used when storing offsets in MQ to know which application the offset is for.  Re-using this across applications will surely result in applications missing messages.
        /// </param>
        public StreamConsumer(string streamName, string consumerApplicationName = "") : base(streamName, EnumStreamType.Consumer)
        {
            if (consumerApplicationName == string.Empty)
                throw new ArgumentException(
                    "The consumerApplicationName must be specified and it must be unique for a given application");
            _consumerApplicationName = consumerApplicationName;
        }


        public string ConsumerApplicationName
        {
            get { return _consumerApplicationName;}
        }


        /// <summary>
        /// The offset of the last message successfully received
        /// </summary>
        public ulong LastOffset { get; private set; }


        /// <summary>
        /// The total number of messages consumed.
        /// </summary>
        public ulong MessagesConsumed
        {
            get { return Counter; }
        }



        /// <summary>
        /// Owners of this class should increment this each time a message is consumed.  And reset it to 0 anytime the offset counter is stored in the message store.
        /// </summary>
        public int OffsetCounter { get; protected set; }


        /// <summary>
        /// How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create your own check method.
        /// </summary>
        public int OffsetCheckPointLimit { get; set; } = 80;



        /// <summary>
        /// Initiate the Consumption cycle
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public async Task ConsumeAsync()
        {
            if (_callHandler == null)
                throw new ApplicationException("Must Set the Call Handler before calling Consume method.");

            // Retrieve any saved offset
            bool offsetFound = false;
            IOffsetType offsetType;
            try
            {
                ulong priorOffset = await _streamSystem.QueryOffset(ConsumerApplicationName, _name);
                offsetType = new OffsetTypeOffset(priorOffset);
                offsetFound = true;
            }
            catch (Exception OffsetNotFoundException)
            {
                offsetType = new OffsetTypeFirst();
            }

            _rawConsumer = await _streamSystem.CreateRawConsumer(new RawConsumerConfig(_name)
            {
                Reference = ConsumerApplicationName,
                OffsetSpec = offsetType,
                MessageHandler = MessageHandler

            });
        }


        /// <summary>
        /// Sets the Method to be called when a message is received.
        /// </summary>
        /// <param name="callHandler"></param>
        public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler) {
            _callHandler = callHandler; 
        }



        /// <summary>
        /// Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
        /// </summary>
        /// <returns></returns>
        private async Task CheckPointSet()
        {
            if (LastOffset == 0) return;

            _rawConsumer.StoreOffset(LastOffset);
            OffsetCounter = 0;
        }


        //(Func<string,RawConsumer,MessageContext,Message,Task> consumptionMethod)



        /// <summary>
        /// This is the method that receives the messages.  It then calls the Call Handler that actually handles the messages.
        /// </summary>
        /// <param name="streamName">Name of the stream the message came in on</param>
        /// <param name="consumer">The Consumer Stream</param>
        /// <param name="msgContext"></param>
        /// <param name="message">The message from the MQ Broker</param>
        /// <returns></returns>
        private async Task MessageHandler(RawConsumer consumer, MessageContext msgContext, Message message)
        {
            //string msg = Encoding.Default.GetString(message.Data.Contents.ToArray());
            bool success = await _callHandler(message);
            if (success)
            {
                Counter++;
                OffsetCounter++;
                LastOffset = msgContext.Offset;

                // TODO - Ideally this moves out of here and into a background thread that periodically checks to see if we should checkpoint.
                if (OffsetCounter >= OffsetCheckPointLimit)
                {
                    CheckPointSet();
                }

            }
            await Task.CompletedTask;
            
        }
        
    }
}
