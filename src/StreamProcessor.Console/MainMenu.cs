using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;
using StreamProcessor.Console;
using StreamProcessor.Console.SampleA;
using StreamProcessor.ConsoleScr.SampleB;
using StreamProcessor.ConsoleScr.SampleC;


namespace StreamProcessor.ConsoleScr
{
    internal class MainMenu
    {
        private readonly ILogger _logger;
        private IServiceProvider _serviceProvider;

        public MainMenu(ILogger<MainMenu> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
        }



        public async Task Display()
        {

            try
            {
                System.Console.WriteLine("MQ Stream Sender");




                // See if a configuration file exists.  If so read from it, otherwise start a new config
                Config config;
                string fileName = "Console.config";
                if (!File.Exists(fileName))
                    config = new Config();
                else
                {
                    using FileStream fileStream = File.OpenRead(fileName);
                    config = await JsonSerializer.DeserializeAsync<Config>(fileStream);
                }


                System.Console.WriteLine("Select which Program you wish to run.");
                System.Console.WriteLine(" ( 1 )  sample_stream - with Defined Queue Settings.");
                System.Console.WriteLine(" ( 2 )  s2 - with no defined Queue Settings.");
                System.Console.WriteLine(" ( 3 )  s3.1MinQueue - Very small Queue size and age limits.");
                System.Console.WriteLine(" ( A )  Sample A Logic.");
                System.Console.WriteLine(" ( B )  Sample B - Multiple Different Simultaneous Consumers");
                System.Console.WriteLine(" ( 0 )  Test Batches Logic.");
                ConsoleKeyInfo key = System.Console.ReadKey();


                string streamName = "";
                IMqStreamProducer producer = null;
                IMqStreamConsumer consumer = null;
                bool deleteStream = false;


                switch (key.Key)
                {
                    case ConsoleKey.A:
                        SampleA sampleA = new SampleA(4);
                        await sampleA.Start();
                        break;

                    case ConsoleKey.B:
                        SampleBApp sampleB = (SampleBApp)_serviceProvider.GetService(typeof(SampleBApp));
                        sampleB.MessagesPerBatch = 6;
                        await sampleB.Start();
                        break;

                    case ConsoleKey.C:
                        SampleCApp sampleC = (SampleCApp)_serviceProvider.GetService(typeof(SampleCApp));
                        await sampleC.Start();
                        break;

                    case ConsoleKey.D0:
                        string batch = "A";
                        for (int j = 0; j < 30; j++)
                        {
                            for (int i = 0; i < 26; i++)
                            {
                                System.Console.WriteLine($"{batch}");
                                batch = HelperFunctions.NextBatch(batch);

                            }
                        }

                        break;

                }

                Thread.Sleep(2000);
                System.Console.WriteLine("Press any key to exit the application.  Press D to delete the stream");
                ConsoleKeyInfo key2 = System.Console.ReadKey();

                if ((key2.Key == ConsoleKey.D) && (deleteStream)) consumer.DeleteStream();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error - {ex.Message}"); 
                _logger.LogError(ex,"Error encountered: ");
            }

            /*
            var producerLogger = loggerFactory.CreateLogger<Producer>();
            var consumerLogger = loggerFactory.CreateLogger<Consumer>();
            var streamLogger = loggerFactory.CreateLogger<StreamSystem>();
            */
        }


        static async Task<bool> ConsumeMessageHandler(Message message)
        {
            //_counter++;
            int _counter = 0;
            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
            System.Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
            await Task.CompletedTask;
            return true;
        }

    }
}

