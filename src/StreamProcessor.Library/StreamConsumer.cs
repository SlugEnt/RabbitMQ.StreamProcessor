
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
        private Consumer _rawConsumer;


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

            await Consumer.Create(
            new ConsumerConfig(_streamSystem, _name)
            {
                Reference = ConsumerApplicationName,

                // Consume the stream from the beginning 
                // See also other OffsetSpec 
                OffsetSpec = offsetType,
                
                // Receive the messages
                //MessageHandler = _callHandler,
                MessageHandler = MessageHandler
            }) ;
        }


        //public long Counter { get; private set; }

        public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler) {
            _callHandler = callHandler; 
        }


        private async Task CheckPointSet(RawConsumer rawConsumer, MessageContext messageContext)
        {
            await rawConsumer.StoreOffset(messageContext.Offset);
            OffsetCounter = 0;
        }


        private async Task CheckPointSet(MessageContext messageContext)
        {
            
            //await  StoreOffset(messageContext.Offset);
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
        private async Task MessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
        {
            string msg = Encoding.Default.GetString(message.Data.Contents.ToArray());
            bool success = await _callHandler(message);
            if (success)
            {
                Counter++;
                OffsetCounter++;
                LastOffset = msgContext.Offset;

                // TODO - Ideally this moves out of here and into a background thread that periodically checks to see if we should checkpoint.
                if (OffsetCounter >= OffsetCheckPointLimit)
                {
                    CheckPointSet(consumer, msgContext);
                }

            }
            await Task.CompletedTask;
            
        }
        
    }
}
