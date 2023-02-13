using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;

namespace StreamProcessor.ConsoleScr.SampleB
{
    public class SampleB_Consumer : MqStreamConsumer
    {
        private Func<Message, Task<bool>> _consumptionHandler;


        public SampleB_Consumer(string streamName, string appName, Func<Message, Task<bool>> consumptionHandler) : base(streamName, appName)
        {
            _consumptionHandler = consumptionHandler;
        }


        public async Task Start()
        {
            await ConnectAsync();
            SetConsumptionHandler(_consumptionHandler);
            await ConsumeAsync();
        }
    }
}
