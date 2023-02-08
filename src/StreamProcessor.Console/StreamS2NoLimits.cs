using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.Console
{
    internal class StreamS2NoLimits
    {
        readonly string _streamName = "sample_stream";
        readonly StreamProducer _producer = null;
        readonly StreamConsumer _consumer = null;
        private List<string> _messages = new List<string>();
        private int _counter = 0;


        internal StreamS2NoLimits()
        {
            _producer = new StreamProducer(_streamName);
            _consumer = new StreamConsumer(_streamName);
        }


        internal async Task ExecuteAsync()
        {
            await _producer.ConnectAsync();
            await _consumer.ConnectAsync();

            // Start producing messages
            await _producer.PublishAsync();



            // Publish the messages
            for (var i = 0; i < 2; i++)
            {
                DateTime x = DateTime.Now;
                string time = x.ToShortTimeString();
                string msg = String.Format("Time: {0}   -->  Msg # {1}", time, i);
                _producer.SendMessage(msg);
            }



            // Start Consuming messages
            System.Console.WriteLine("Starting to Consume Messages");
            _consumer.SetConsumptionHandler(ConsumeMessageHandler);
            await _consumer.ConsumeAsync();



            // Now list the messages out.
            Thread.Sleep(2000);
            int j = 0;
            foreach (string msg in _messages)
            {
                j++;
                System.Console.WriteLine($"Received Msg: #{j}  Contents: {msg}");
            }
            
        }




        public async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
        {
            //_counter++;
            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
            _messages.Add(x);
            //Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
            await Task.CompletedTask;
        }


    }
}
