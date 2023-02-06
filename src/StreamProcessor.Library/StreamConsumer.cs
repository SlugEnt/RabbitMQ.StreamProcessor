
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Text;

namespace SlugEnt.StreamProcessor
{
    public class StreamConsumer : StreamBase
    {
       
        private int _counter;
        private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;


        public StreamConsumer(string name) : base(name) { }

        public async Task ConsumeAsync()
        {
            int consumeCtr = 0;

            if (_callHandler == null) throw new ApplicationException("Must Set the Call Handler before calling Consume method.");

            // Connect to the Stream
            _streamSystem.CreateStream(_streamSpec);

            var consumer = await Consumer.Create(
                new ConsumerConfig(_streamSystem, _name)
                {
                    Reference = "my_consumer",

                    // Consume the stream from the beginning 
                    // See also other OffsetSpec 
                    OffsetSpec = new OffsetTypeFirst(),

                    // Receive the messages
                    MessageHandler = _callHandler,
                    //        MessageHandler = ConsumeMessageHandler,
                }) ;
        }

        public void SetConsumptionHandler(Func<string, RawConsumer, MessageContext, Message, Task> callHandler) {
            _callHandler = callHandler; 
        }


        //(Func<string,RawConsumer,MessageContext,Message,Task> consumptionMethod)
        public async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
        {
            _counter++;
            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());

            //Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
            await Task.CompletedTask;

        }

        /*
                public async Task ConsumeMessageHandler (string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
                {
                    _counter++;
                    string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
                    Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
                    await Task.CompletedTask;

                }
        */
    }
}
