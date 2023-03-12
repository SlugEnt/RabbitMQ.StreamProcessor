using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.MQStreamProcessor;

namespace StreamProcessor.ConsoleScr.SampleB
{
    public interface ISampleB_Consumer : IMqStreamConsumer
    {
        Task Start();
    }


    public class SampleB_Consumer : MqStreamConsumer, ISampleB_Consumer
    {
        //private Func<Message, Task<bool>> _consumptionHandler;
        private ILogger<SampleB_Consumer> _logger;


        public SampleB_Consumer(ILogger<SampleB_Consumer> logger, ServiceProvider serviceProvider) : base(logger, serviceProvider)
        {
            _logger = logger;

            //_consumptionHandler = consumptionHandler;
        }



        public async Task Start()
        {
            try
            {
                await ConnectAsync();
                await ConsumeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                System.Console.WriteLine("There was an error - {0}", ex.Message);
            }
        }
    }
}