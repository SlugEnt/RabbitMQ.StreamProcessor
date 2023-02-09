
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Text;

namespace SlugEnt.StreamProcessor
{
    public class StreamConsumer : StreamBase
    {
        private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;
        private readonly string _consumerApplicationName;


        /// <summary>
        /// Creates a Stream Consumer object
        /// </summary>
        /// <param name="streamName">The name of the RabbitMQ Queue (stream) name to receive messages from</param>
        /// <param name="consumerApplicationName">Unique name for the application processing this stream.  This is used when storing offsets in MQ to know which application the offset is for.  Re-using this across applications will surely result in applications missing messages.
        /// </param>
        public StreamConsumer(string streamName, string consumerApplicationName = "") : base(streamName,
            EnumStreamType.Consumer)
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
        /// Owners of this class should increment this each time a message is consumed.  And reset it to 0 anytime the offset counter is stored in the message store.
        /// </summary>
        public int OffsetCounter { get; set; }


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
            int consumeCtr = 0;

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

            var consumer = await Consumer.Create(
            new ConsumerConfig(_streamSystem, _name)
            {
                Reference = ConsumerApplicationName,

                // Consume the stream from the beginning 
                // See also other OffsetSpec 
                
                OffsetSpec = offsetType,
                
                // Receive the messages
                MessageHandler = _callHandler,
            }) ;
        }

        //public long Counter { get; private set; }

        public void SetConsumptionHandler(Func<string, RawConsumer, MessageContext, Message, Task> callHandler) {
            _callHandler = callHandler; 
        }


        public async Task CheckpointOffset(RawConsumer rawConsumer, MessageContext messageContext)
        {
            await rawConsumer.StoreOffset(messageContext.Offset);
            OffsetCounter = 0;
        }

        //(Func<string,RawConsumer,MessageContext,Message,Task> consumptionMethod)
        public async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
        {
          //  Counter++;
            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());

            //Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
            await Task.CompletedTask;

        }
        
    }
}
